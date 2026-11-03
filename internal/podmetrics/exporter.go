// Package podmetrics exports a deleted Spark pod's (driver or executor)
// Prometheus metrics to Elasticsearch. The handler hands over the deleted
// pod; the exporter owns the slow path (range queries across the pod's whole
// lifecycle plus Elasticsearch indexing) on background workers so informer
// callbacks never block on external I/O.
package podmetrics

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"podwatcher/internal/prometheus"
	"podwatcher/internal/store"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const (
	// Defaults for the tunable queue/worker/step sizing; invalid
	// configuration falls back to these.
	defaultQueueSize = 256
	defaultWorkers   = 2
	defaultStep      = time.Minute
	// Whole-export budget: several range queries plus one index request.
	processTimeout = 5 * time.Minute
)

// Option customizes the exporter sizing.
type Option func(*Exporter)

// WithQueueSize sets the pending-export queue capacity; non-positive values
// keep the default.
func WithQueueSize(size int) Option {
	return func(e *Exporter) {
		if size > 0 {
			e.queue = make(chan *corev1.Pod, size)
		}
	}
}

// WithWorkers sets the number of concurrent export workers; non-positive
// values keep the default.
func WithWorkers(n int) Option {
	return func(e *Exporter) {
		if n > 0 {
			e.workers = n
		}
	}
}

// WithStep sets the Prometheus range-query sampling step; non-positive
// values keep the default.
func WithStep(d time.Duration) Option {
	return func(e *Exporter) {
		if d > 0 {
			e.step = d
		}
	}
}

// QuerySpec is one configured metric export: the PromQL template plus an
// optional accumulation flag.
type QuerySpec struct {
	// Expr is a PromQL template with {namespace}/{pod} placeholders.
	Expr string
	// Accumulate replaces the per-step values with their running time
	// integral (value x step), turning a gauge such as working-set MB into
	// a cumulative MB-seconds curve.
	Accumulate bool
}

// WithQueries sets the configured query specs; a nil map keeps the previous
// value.
func WithQueries(queries map[string]QuerySpec) Option {
	return func(e *Exporter) {
		if queries != nil {
			e.queries = queries
		}
	}
}

// Querier is the subset of the Prometheus client the exporter needs.
type Querier interface {
	QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration) ([]prometheus.Result, error)
}

// Sink persists one pod's exported metrics document.
type Sink interface {
	PublishPodMetrics(ctx context.Context, doc PodMetricsDocument) error
}

// PodMetricsDocument carries a Spark pod's identity plus the metrics
// collected over its lifecycle. The Metrics keys are the configured export
// names (podMetricsExport.queries), each holding that query's full series.
type PodMetricsDocument struct {
	ApplicationID string                         `json:"applicationId"`
	Namespace     string                         `json:"namespace"`
	PodName       string                         `json:"podName"`
	Role          string                         `json:"role"` // driver or executor
	Node          string                         `json:"node"`
	WindowStart   time.Time                      `json:"windowStart"`
	WindowEnd     time.Time                      `json:"windowEnd"`
	Metrics       map[string][]prometheus.Result `json:"metrics,omitempty"`
}

// Exporter accepts deleted Spark pods and exports their lifecycle metrics.
// PublishPodMetrics only snapshots and enqueues; workers run the queries and
// the Elasticsearch write.
type Exporter struct {
	querier Querier
	sink    Sink
	queue   chan *corev1.Pod
	workers int
	// queries are the configured query specs; step is the range-query
	// sampling interval.
	queries map[string]QuerySpec
	step    time.Duration
	// inflight deduplicates re-delivered OnDelete events while a pod is
	// queued or being processed. After completion, redeliveries rely on the
	// sink's stable document id for idempotency.
	inflight sync.Map
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	// buildQueries returns the query selection for one pod. A field so tests
	// can inject queries; the production value renders the configured specs
	// (see renderQueries).
	buildQueries func(*corev1.Pod) map[string]QuerySpec
}

// NewExporter builds an exporter; call Start to launch the workers.
func NewExporter(querier Querier, sink Sink, opts ...Option) *Exporter {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Exporter{
		querier: querier,
		sink:    sink,
		queue:   make(chan *corev1.Pod, defaultQueueSize),
		workers: defaultWorkers,
		step:    defaultStep,
		ctx:     ctx,
		cancel:  cancel,
	}
	for _, opt := range opts {
		opt(e)
	}
	if e.buildQueries == nil {
		e.buildQueries = renderQueries(e.queries)
	}
	e.observeQueueDepth()
	return e
}

// Start launches the worker pool.
func (e *Exporter) Start() {
	for i := 0; i < e.workers; i++ {
		e.wg.Add(1)
		go e.work()
	}
}

// Stop cancels the workers and waits for the current export to finish.
// Queued pods are abandoned with their Prometheus history intact — a later
// reconciliation pass can re-export them.
func (e *Exporter) Stop() {
	e.cancel()
	e.wg.Wait()
}

// observeQueueDepth registers an observable gauge reporting the pending
// export backlog. The global meter provider's periodic reader reads the
// callback on every collection interval (60s by default), so the OTLP
// collector receives a queue-depth snapshot per interval. Without a
// configured provider (tracing.endpoint empty) the instrument is a no-op.
func (e *Exporter) observeQueueDepth() {
	meter := otel.Meter("podwatcher")
	gauge, err := meter.Int64ObservableGauge(
		"podwatcher.podmetrics.queue.depth",
		metric.WithDescription("Pod metrics exports buffered and waiting for a worker"),
	)
	if err != nil {
		klog.Warningf("Failed to create pod metrics queue depth gauge: %v", err)
		return
	}
	if _, err := meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		observer.ObserveInt64(gauge, int64(len(e.queue)))
		return nil
	}, gauge); err != nil {
		klog.Warningf("Failed to register pod metrics queue depth callback: %v", err)
	}
}

// PublishPodMetrics accepts a deleted pod for export. It only snapshots the
// pod and enqueues the work, so it is safe to call from informer callbacks.
func (e *Exporter) PublishPodMetrics(_ context.Context, pod *corev1.Pod) error {
	select {
	case <-e.ctx.Done():
		return fmt.Errorf("pod metrics exporter stopped")
	default:
	}
	key := podIdentity(pod)
	if _, loaded := e.inflight.LoadOrStore(key, struct{}{}); loaded {
		return nil
	}
	// Cheap pre-check before paying for a full deep copy: a saturated queue
	// has nothing to schedule. The race is benign — a queue that frees up
	// between check and send just drops one more pod in the fallback below.
	if len(e.queue) == cap(e.queue) {
		e.inflight.Delete(key)
		klog.Warningf("Pod metrics export queue full, dropping %s", key)
		return fmt.Errorf("pod metrics export queue is full")
	}
	// The informer may hand over a shared cache object; snapshot it for
	// asynchronous use.
	snapshot := pod.DeepCopy()
	select {
	case e.queue <- snapshot:
		return nil
	default:
		e.inflight.Delete(key)
		klog.Warningf("Pod metrics export queue full, dropping %s", key)
		return fmt.Errorf("pod metrics export queue is full")
	}
}

func (e *Exporter) work() {
	defer e.wg.Done()
	for {
		select {
		case <-e.ctx.Done():
			return
		case pod := <-e.queue:
			e.process(pod)
			e.inflight.Delete(podIdentity(pod))
		}
	}
}

// process runs one export: lifecycle window, range queries, index the
// aggregated document. Failures are logged and dropped — Prometheus retains
// the raw samples, so a later reconciliation pass can retry.
func (e *Exporter) process(pod *corev1.Pod) {
	ctx, cancel := context.WithTimeout(e.ctx, processTimeout)
	defer cancel()
	start, end := lifecycleWindow(pod)
	if start.IsZero() || !start.Before(end) {
		klog.Warningf("Pod metrics export skipped for %s: empty lifecycle window (start=%v end=%v)",
			podIdentity(pod), start, end)
		return
	}
	queries := e.buildQueries(pod)
	if len(queries) == 0 {
		// Reached only until the PromQL selection is implemented; keeps the
		// wiring observable without writing placeholder documents.
		klog.Infof("Pod metrics export has no queries configured, skipping %s/%s", pod.Namespace, pod.Name)
		return
	}
	doc := PodMetricsDocument{
		ApplicationID: store.AppIDFromPod(pod),
		Namespace:     pod.Namespace,
		PodName:       pod.Name,
		Role:          store.SparkRole(pod),
		Node:          pod.Spec.NodeName,
		WindowStart:   start,
		WindowEnd:     end,
		Metrics:       make(map[string][]prometheus.Result, len(queries)),
	}
	for name, spec := range queries {
		results, err := e.querier.QueryRange(ctx, spec.Expr, start, end, e.step)
		if err != nil {
			klog.Errorf("Pod metrics query %q failed for %s/%s: %v", name, pod.Namespace, pod.Name, err)
			return
		}
		if spec.Accumulate {
			results, err = accumulate(results, e.step)
			if err != nil {
				klog.Errorf("Pod metrics query %q cannot be accumulated for %s/%s: %v", name, pod.Namespace, pod.Name, err)
				return
			}
		}
		doc.Metrics[name] = results
	}
	if err := e.sink.PublishPodMetrics(ctx, doc); err != nil {
		klog.Errorf("Failed to publish pod metrics for %s/%s: %v", pod.Namespace, pod.Name, err)
	}
}

// lifecycleWindow bounds the Prometheus range query. The window ends at the
// moment the pod stopped producing samples — the earliest recorded container
// termination time, falling back to the deletion timestamp and then to now.
// Preferring the termination time avoids indexing the post-termination
// zero-sample tail between completion and object deletion.
func lifecycleWindow(pod *corev1.Pod) (time.Time, time.Time) {
	start := pod.CreationTimestamp.Time
	end := time.Time{}
	if pod.DeletionTimestamp != nil {
		end = pod.DeletionTimestamp.Time
	}
	for _, status := range pod.Status.ContainerStatuses {
		terminated := status.State.Terminated
		if terminated == nil || terminated.FinishedAt.IsZero() {
			continue
		}
		finished := terminated.FinishedAt.Time
		if end.IsZero() || finished.Before(end) {
			end = finished
		}
	}
	if end.IsZero() {
		end = time.Now()
	}
	return start, end
}

// accumulate converts a per-step quantity into its running time integral:
// each point becomes the sum of all prior values multiplied by the step, i.e.
// a cumulative <unit>-seconds curve. Prometheus has no cumulative operator,
// so gauge-to-counter derivation (e.g. working-set MB -> MB-seconds) happens
// here. Samples are expected in ascending time order, as returned by the
// range API.
func accumulate(results []prometheus.Result, step time.Duration) ([]prometheus.Result, error) {
	seconds := step.Seconds()
	out := make([]prometheus.Result, 0, len(results))
	for _, r := range results {
		values := make([]prometheus.Sample, 0, len(r.Values))
		var total float64
		for _, s := range r.Values {
			v, err := strconv.ParseFloat(s.Value, 64)
			// Prometheus emits "NaN" for range gaps; ParseFloat accepts it, so
			// finiteness is checked explicitly — a missing sample would
			// otherwise silently undercount the integral.
			if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
				return nil, fmt.Errorf("non-finite sample %q", s.Value)
			}
			total += v * seconds
			values = append(values, prometheus.Sample{
				Timestamp: s.Timestamp,
				Value:     strconv.FormatFloat(total, 'f', -1, 64),
			})
		}
		r.Values = values
		r.Value = nil
		out = append(out, r)
	}
	return out, nil
}

// podIdentity mirrors the Elasticsearch client's convention: the pod UID
// when present, otherwise namespace/name.
func podIdentity(pod *corev1.Pod) string {
	if pod.UID != "" {
		return string(pod.UID)
	}
	return pod.Namespace + "/" + pod.Name
}

// renderQueries returns the production buildQueries: it renders the
// configured query expressions per pod, substituting {namespace} and {pod}.
// An empty spec set yields no queries, keeping the pipeline inert (logged
// skips, no documents) until queries are configured.
func renderQueries(specs map[string]QuerySpec) func(*corev1.Pod) map[string]QuerySpec {
	return func(pod *corev1.Pod) map[string]QuerySpec {
		if len(specs) == 0 {
			return nil
		}
		replacer := strings.NewReplacer("{namespace}", pod.Namespace, "{pod}", pod.Name)
		queries := make(map[string]QuerySpec, len(specs))
		for name, spec := range specs {
			queries[name] = QuerySpec{Expr: replacer.Replace(spec.Expr), Accumulate: spec.Accumulate}
		}
		return queries
	}
}
