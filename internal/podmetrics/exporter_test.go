package podmetrics

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"podwatcher/internal/prometheus"
	"podwatcher/internal/testutil"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type fakeQuerier struct {
	entered chan struct{} // closed when the first query starts
	release chan struct{} // blocks queries until closed
	queries []string
	results []prometheus.Result
	err     error
}

func (q *fakeQuerier) QueryRange(_ context.Context, query string, _, _ time.Time, _ time.Duration) ([]prometheus.Result, error) {
	if q.entered != nil {
		close(q.entered)
		q.entered = nil
	}
	if q.release != nil {
		<-q.release
	}
	q.queries = append(q.queries, query)
	return q.results, q.err
}

type fakeSink struct {
	docs chan PodMetricsDocument
	err  error
}

func (s *fakeSink) PublishPodMetrics(_ context.Context, doc PodMetricsDocument) error {
	s.docs <- doc
	return s.err
}

// deletedDriver builds a succeeded driver pod deleted one minute after its
// container terminated, with second-precision timestamps for exact compares.
func deletedDriver(uid string) *corev1.Pod {
	created := time.Now().Add(-time.Hour).Truncate(time.Second)
	finished := created.Add(30 * time.Minute)
	pod := testutil.WithUID(testutil.DriverPod("app-e-driver", "app-e", corev1.PodSucceeded, created), uid)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "main",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.NewTime(finished)}},
	}}
	pod.DeletionTimestamp = &metav1.Time{Time: finished.Add(time.Minute)}
	return pod
}

func withQueries(specs map[string]QuerySpec) func(*corev1.Pod) map[string]QuerySpec {
	return func(*corev1.Pod) map[string]QuerySpec { return specs }
}

func TestExporterPublishesLifecycleMetrics(t *testing.T) {
	created := time.Now().Add(-time.Hour).Truncate(time.Second)
	finished := created.Add(30 * time.Minute)
	querier := &fakeQuerier{results: []prometheus.Result{{Metric: map[string]string{"container": "main"}}}}
	sink := &fakeSink{docs: make(chan PodMetricsDocument, 1)}
	e := NewExporter(querier, sink)
	e.buildQueries = withQueries(map[string]QuerySpec{"cpu": {Expr: "sum(rate(container_cpu_usage[5m]))"}})
	e.Start()
	defer e.Stop()
	if err := e.PublishPodMetrics(context.Background(), deletedDriver("uid-1")); err != nil {
		t.Fatal(err)
	}
	var doc PodMetricsDocument
	select {
	case doc = <-sink.docs:
	case <-time.After(2 * time.Second):
		t.Fatal("export did not complete")
	}
	if doc.ApplicationID != "app-e" || doc.PodName != "app-e-driver" || doc.Role != "driver" || doc.Node != "node-1" {
		t.Fatalf("incorrect identity: %+v", doc)
	}
	if !doc.WindowStart.Equal(created) || !doc.WindowEnd.Equal(finished) {
		t.Fatalf("window must end at container termination, got %v..%v", doc.WindowStart, doc.WindowEnd)
	}
	if len(doc.Metrics["cpu"]) != 1 || len(querier.queries) != 1 || querier.queries[0] != "sum(rate(container_cpu_usage[5m]))" {
		t.Fatalf("incorrect metrics: %+v / %v", doc.Metrics, querier.queries)
	}
}

func TestExporterDeduplicatesInflightPods(t *testing.T) {
	querier := &fakeQuerier{entered: make(chan struct{}), release: make(chan struct{})}
	sink := &fakeSink{docs: make(chan PodMetricsDocument, 1)}
	e := NewExporter(querier, sink)
	e.buildQueries = withQueries(map[string]QuerySpec{"cpu": {Expr: "q"}})
	e.Start()
	defer e.Stop()
	pod := deletedDriver("uid-2")
	if err := e.PublishPodMetrics(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	select {
	case <-querier.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never started the export")
	}
	// A re-delivered OnDelete while the first export is in flight is a no-op.
	if err := e.PublishPodMetrics(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	close(querier.release)
	<-sink.docs
	if len(querier.queries) != 1 {
		t.Fatalf("duplicate export must be dropped, got %d queries", len(querier.queries))
	}
}

func TestExporterDropsExportOnQueryFailure(t *testing.T) {
	querier := &fakeQuerier{err: errors.New("prometheus unavailable")}
	sink := &fakeSink{docs: make(chan PodMetricsDocument, 1)}
	e := NewExporter(querier, sink)
	e.buildQueries = withQueries(map[string]QuerySpec{"cpu": {Expr: "q"}})
	e.process(deletedDriver("uid-3"))
	select {
	case doc := <-sink.docs:
		t.Fatalf("failed export must not index a document: %+v", doc)
	default:
	}
}

func TestExporterSkipsWithoutQueries(t *testing.T) {
	querier := &fakeQuerier{}
	sink := &fakeSink{docs: make(chan PodMetricsDocument, 1)}
	e := NewExporter(querier, sink)
	e.process(deletedDriver("uid-4"))
	select {
	case doc := <-sink.docs:
		t.Fatalf("no queries configured must not index: %+v", doc)
	default:
	}
	if len(querier.queries) != 0 {
		t.Fatal("no queries configured must not call Prometheus")
	}
}

func TestExporterDropsWhenQueueFull(t *testing.T) {
	e := NewExporter(&fakeQuerier{}, &fakeSink{docs: make(chan PodMetricsDocument, 1)})
	e.queue = make(chan *corev1.Pod, 1)
	e.queue <- deletedDriver("uid-5")
	if err := e.PublishPodMetrics(context.Background(), deletedDriver("uid-6")); err == nil {
		t.Fatal("overflow must be reported")
	}
}

func TestExporterDedupWinsOverFullQueue(t *testing.T) {
	e := NewExporter(&fakeQuerier{}, &fakeSink{docs: make(chan PodMetricsDocument, 1)})
	e.queue = make(chan *corev1.Pod, 1)
	e.queue <- deletedDriver("uid-8")
	pod := deletedDriver("uid-7")
	e.inflight.Store(podIdentity(pod), struct{}{})
	if err := e.PublishPodMetrics(context.Background(), pod); err != nil {
		t.Fatalf("inflight pod must dedup quietly despite full queue: %v", err)
	}
}

func TestExporterSizingOptions(t *testing.T) {
	e := NewExporter(&fakeQuerier{}, &fakeSink{docs: make(chan PodMetricsDocument, 1)},
		WithQueueSize(7), WithWorkers(3))
	defer e.Stop()
	if cap(e.queue) != 7 || e.workers != 3 {
		t.Fatalf("options not applied: cap=%d workers=%d", cap(e.queue), e.workers)
	}
	// Invalid values keep the defaults.
	e2 := NewExporter(&fakeQuerier{}, &fakeSink{docs: make(chan PodMetricsDocument, 1)},
		WithQueueSize(0), WithWorkers(-1), WithStep(0))
	defer e2.Stop()
	if cap(e2.queue) != defaultQueueSize || e2.workers != defaultWorkers || e2.step != defaultStep {
		t.Fatalf("invalid values must keep defaults: cap=%d workers=%d step=%v", cap(e2.queue), e2.workers, e2.step)
	}
}

func TestConfiguredQueriesAreRenderedPerPod(t *testing.T) {
	e := NewExporter(&fakeQuerier{}, &fakeSink{docs: make(chan PodMetricsDocument, 1)},
		WithQueries(map[string]QuerySpec{
			"cpu": {Expr: `rate(container_cpu_usage_seconds_total{namespace="{namespace}",pod="{pod}"}[5m])`},
		}),
		WithStep(2*time.Minute))
	defer e.Stop()
	pod := testutil.DriverPod("d-1", "app-x", corev1.PodRunning, time.Now())
	pod.Namespace = "ns-x"
	got := e.buildQueries(pod)
	want := `rate(container_cpu_usage_seconds_total{namespace="ns-x",pod="d-1"}[5m])`
	if got["cpu"].Expr != want {
		t.Fatalf("template not rendered: got %q want %q", got["cpu"].Expr, want)
	}
	if e.step != 2*time.Minute {
		t.Fatalf("step option not applied: %v", e.step)
	}
	// The test-injected buildQueries survives option-time derivation.
	e.buildQueries = withQueries(map[string]QuerySpec{"cpu": {Expr: "q"}})
	if q := e.buildQueries(testutil.DriverPod("d-2", "app-x", corev1.PodRunning, time.Now())); q["cpu"].Expr != "q" {
		t.Fatalf("injected buildQueries must win: %v", q)
	}
}

func TestAccumulateBuildsRunningIntegral(t *testing.T) {
	// 10, 20, 30 MB at three consecutive steps with step=60s accumulate to
	// 600, 1800, 3600 MB-seconds.
	querier := &fakeQuerier{results: []prometheus.Result{{
		Metric: map[string]string{"container": "main"},
		Values: []prometheus.Sample{
			{Timestamp: 1, Value: "10"},
			{Timestamp: 61, Value: "20"},
			{Timestamp: 121, Value: "30"},
		},
	}}}
	sink := &fakeSink{docs: make(chan PodMetricsDocument, 1)}
	e := NewExporter(querier, sink)
	e.buildQueries = withQueries(map[string]QuerySpec{"memoryMbSeconds": {Expr: "q", Accumulate: true}})
	e.process(deletedDriver("uid-9"))
	var doc PodMetricsDocument
	select {
	case doc = <-sink.docs:
	case <-time.After(2 * time.Second):
		t.Fatal("export did not complete")
	}
	got := doc.Metrics["memoryMbSeconds"][0].Values
	for i, want := range []float64{600, 1800, 3600} {
		if v, err := strconv.ParseFloat(got[i].Value, 64); err != nil || v != want {
			t.Fatalf("point %d: got %v (%q) want %v", i, v, got[i].Value, want)
		}
	}
}

func TestAccumulateRejectsNonNumericSamples(t *testing.T) {
	querier := &fakeQuerier{results: []prometheus.Result{{
		Metric: map[string]string{"container": "main"},
		Values: []prometheus.Sample{{Timestamp: 1, Value: "NaN"}},
	}}}
	sink := &fakeSink{docs: make(chan PodMetricsDocument, 1)}
	e := NewExporter(querier, sink)
	e.buildQueries = withQueries(map[string]QuerySpec{"memoryMbSeconds": {Expr: "q", Accumulate: true}})
	e.process(deletedDriver("uid-10"))
	select {
	case doc := <-sink.docs:
		t.Fatalf("failed accumulation must not index a document: %+v", doc)
	default:
	}
}

func TestLifecycleWindow(t *testing.T) {
	created := time.Now().Add(-time.Hour).Truncate(time.Second)
	finished := created.Add(30 * time.Minute)
	deleted := finished.Add(time.Minute)

	pod := testutil.DriverPod("driver", "app", corev1.PodSucceeded, created)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "main",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.NewTime(finished)}},
	}}
	pod.DeletionTimestamp = &metav1.Time{Time: deleted}
	start, end := lifecycleWindow(pod)
	if !start.Equal(created) || !end.Equal(finished) {
		t.Fatalf("termination time must win over deletion: %v..%v", start, end)
	}

	pod.Status.ContainerStatuses = nil
	if _, end = lifecycleWindow(pod); !end.Equal(deleted) {
		t.Fatalf("deletion timestamp fallback: got %v want %v", end, deleted)
	}

	pod.DeletionTimestamp = nil
	start, end = lifecycleWindow(pod)
	if start.IsZero() || end.Before(start) {
		t.Fatalf("no signals must fall back to now: %v..%v", start, end)
	}

	if start, _ = lifecycleWindow(testutil.DriverPod("driver", "app", corev1.PodRunning, time.Time{})); !start.IsZero() {
		t.Fatal("zero creation time must be surfaced for skipping")
	}
}
