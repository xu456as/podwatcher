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
	"sort"
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
	// shortWindowStep samples pods whose lifecycle window is under the
	// configured threshold, so a short-lived executor still yields a few
	// points instead of one or two.
	shortWindowStep = 30 * time.Second
	// defaultStepThreshold applies when the configuration sets no threshold.
	defaultStepThreshold = 5 * time.Minute
	// Whole-export budget: several range queries plus one index request.
	processTimeout = 5 * time.Minute
)

// windowBuckets are the explicit histogram boundaries (in seconds) for the
// pod lifecycle-window distribution: 1min, 5min, 10min, 30min, 1h, 2h, 5h,
// 10h, 1day, 2days, 5days.
var windowBuckets = []float64{
	1 * 60, 5 * 60, 10 * 60, 30 * 60,
	1 * 3600, 2 * 3600, 5 * 3600, 10 * 3600,
	24 * 3600, 48 * 3600, 5 * 24 * 3600,
}

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

// WithStepThreshold sets the lifecycle-window length below which queries use
// a fixed 30s step instead of the configured step; non-positive values keep
// the default.
func WithStepThreshold(d time.Duration) Option {
	return func(e *Exporter) {
		if d > 0 {
			e.stepThreshold = d
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
// collected over its lifecycle.
type PodMetricsDocument struct {
	ApplicationID string         `json:"applicationId"`
	Namespace     string         `json:"namespace"`
	PodName       string         `json:"podName"`
	Role          string         `json:"role"` // driver or executor
	Node          string         `json:"node"`
	WindowStart   time.Time      `json:"windowStart"`
	WindowEnd     time.Time      `json:"windowEnd"`
	Metrics       []MetricSeries `json:"metrics,omitempty"`
}

// MetricSeries is one Prometheus series belonging to one configured export
// (podMetricsExport.queries). The export name is a field value rather than a
// JSON object key so the Elasticsearch mapping stays fixed no matter how many
// queries are configured: an extra query grows the document, not the mapping.
type MetricSeries struct {
	Name   string              `json:"name"`
	Metric map[string]string   `json:"metric,omitempty"`
	Values []prometheus.Sample `json:"values"`
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
	// stepThreshold is the lifecycle-window length below which queries use
	// shortWindowStep instead of step.
	stepThreshold time.Duration
	// inflight deduplicates re-delivered OnDelete events while a pod is
	// queued or being processed. After completion, a redelivery exports the
	// pod again and the sink inserts a second document; the two records are
	// identical apart from indexedAt, so consumers must deduplicate by pod.
	inflight sync.Map
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	// buildQueries returns the query selection for one pod. A field so tests
	// can inject queries; the production value renders the configured specs
	// (see renderQueries).
	buildQueries func(*corev1.Pod) map[string]QuerySpec
	// durationHistogram records each export's wall-clock latency; nil when
	// the global meter provider is not configured (OTel no-op).
	durationHistogram metric.Float64Histogram
	// windowHistogram records the distribution of exported pods' lifecycle
	// windows (windowEnd - windowStart); nil only if instrument creation
	// failed.
	windowHistogram metric.Float64Histogram
	// duration accumulates export latencies between metric collections; the
	// gauges registered in initDurationMetrics take and clear it, so every
	// scrape reports only the exports finished during its own interval.
	duration durationStats
}

// NewExporter builds an exporter; call Start to launch the workers.
func NewExporter(querier Querier, sink Sink, opts ...Option) *Exporter {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Exporter{
		querier:       querier,
		sink:          sink,
		queue:         make(chan *corev1.Pod, defaultQueueSize),
		workers:       defaultWorkers,
		step:          defaultStep,
		stepThreshold: defaultStepThreshold,
		ctx:           ctx,
		cancel:        cancel,
	}
	for _, opt := range opts {
		opt(e)
	}
	if e.buildQueries == nil {
		e.buildQueries = renderQueries(e.queries)
	}
	e.observeQueueDepth()
	e.initHistogram()
	e.initWindowHistogram()
	e.initDurationMetrics()
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

// initHistogram creates a histogram recording the end-to-end export latency
// for each pod: range queries + optional accumulation + Elasticsearch write.
// The OTel SDK's default bucket boundaries (under 10s) are a reasonable fit
// for Prometheus range queries. Its _count and _sum are cumulative across
// scrapes, which is why initDurationMetrics adds per-interval gauges next to
// it; callers can safely e.durationHistogram.Record(ctx, d) — a nil
// histogram in no-op mode would panic, hence the nil check at the call site.
func (e *Exporter) initHistogram() {
	meter := otel.Meter("podwatcher")
	h, err := meter.Float64Histogram(
		"podwatcher.podmetrics.export.duration",
		metric.WithDescription("Wall-clock time for one pod's metrics export (range queries + accumulation + ES write)"),
		metric.WithUnit("s"),
	)
	if err != nil {
		klog.Warningf("Failed to create pod metrics export duration histogram: %v", err)
		return
	}
	e.durationHistogram = h
}

// initWindowHistogram creates a histogram over exported pods' lifecycle
// windows (windowEnd - windowStart) with the explicit windowBuckets
// boundaries, so the distribution of short-lived executors vs long-running
// drivers is visible per scrape. Recorded after the empty-window check, so
// skipped pods do not count.
func (e *Exporter) initWindowHistogram() {
	meter := otel.Meter("podwatcher")
	h, err := meter.Float64Histogram(
		"podwatcher.podmetrics.window.duration",
		metric.WithDescription("Distribution of exported pods' lifecycle windows (windowEnd - windowStart)"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(windowBuckets...),
	)
	if err != nil {
		klog.Warningf("Failed to create pod metrics window duration histogram: %v", err)
		return
	}
	e.windowHistogram = h
}

// durationStats aggregates export latencies between metric collections. The
// observable gauges in initDurationMetrics take and clear these values on
// every collection, so each scrape reports only the exports that finished
// during the current interval.
type durationStats struct {
	mu    sync.Mutex
	count int64
	sum   float64
	peak  float64
}

// observe adds one finished export's wall-clock seconds to the interval.
func (s *durationStats) observe(seconds float64) {
	s.mu.Lock()
	s.count++
	s.sum += seconds
	if seconds > s.peak {
		s.peak = seconds
	}
	s.mu.Unlock()
}

// take returns the interval aggregates and resets them. The caller becomes
// the sole owner of the data, so with several MeterProvider readers only the
// first collection would see it.
func (s *durationStats) take() (count int64, sum, peak float64) {
	s.mu.Lock()
	count, sum, peak = s.count, s.sum, s.peak
	s.count, s.sum, s.peak = 0, 0, 0
	s.mu.Unlock()
	return count, sum, peak
}

// initDurationMetrics registers per-interval observable gauges next to the
// histogram from initHistogram: how many exports finished since the previous
// collection, their total wall-clock seconds, and the slowest one. The
// histogram's _count and _sum are cumulative and keep growing across scrapes
// even when no export runs, so these gauges complement it with samples that
// are independent — each describes only its own interval. They use their own
// names because the histogram already occupies export.duration.count/sum.
// Without a configured meter provider the instruments are no-ops and nothing
// is collected.
func (e *Exporter) initDurationMetrics() {
	meter := otel.Meter("podwatcher")
	countGauge, err := meter.Int64ObservableGauge(
		"podwatcher.podmetrics.export.interval.count",
		metric.WithDescription("Pod metrics exports completed since the previous collection"),
	)
	if err != nil {
		klog.Warningf("Failed to create pod metrics export duration count gauge: %v", err)
		return
	}
	sumGauge, err := meter.Float64ObservableGauge(
		"podwatcher.podmetrics.export.interval.duration.sum",
		metric.WithDescription("Total wall-clock seconds spent on pod metrics exports since the previous collection"),
		metric.WithUnit("s"),
	)
	if err != nil {
		klog.Warningf("Failed to create pod metrics export duration sum gauge: %v", err)
		return
	}
	peakGauge, err := meter.Float64ObservableGauge(
		"podwatcher.podmetrics.export.interval.duration.max",
		metric.WithDescription("Slowest pod metrics export (range queries + accumulation + ES write) since the previous collection"),
		metric.WithUnit("s"),
	)
	if err != nil {
		klog.Warningf("Failed to create pod metrics export duration max gauge: %v", err)
		return
	}
	if _, err := meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		count, sum, peak := e.duration.take()
		observer.ObserveInt64(countGauge, count)
		observer.ObserveFloat64(sumGauge, sum)
		observer.ObserveFloat64(peakGauge, peak)
		return nil
	}, countGauge, sumGauge, peakGauge); err != nil {
		klog.Warningf("Failed to register pod metrics export duration callback: %v", err)
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
//
// Error-handling policy split by layer:
//   - IO-layer errors (Prometheus QueryRange, Elasticsearch write) fail the
//     whole pod's export — every configured metric gets a gap if one RPC
//     fails, since we can't know how partial success interacts with a later
//     retry (the sink inserts a new document per call, so a half-full one
//     would sit next to the complete retry). These never panic; they just
//     log and return.
//   - Compute-layer errors (accumulate's non-finite-sample rejection) are
//     per-metric isolated: the bad metric keeps a series entry with no values
//     in doc.Metrics and processing continues for the others. A gap like this
//     is usually transient (Prometheus TSDB backfilling a single NaN) and not
//     worth dropping seven other metrics for.
func (e *Exporter) process(pod *corev1.Pod) {
	exportStart := time.Now()
	ctx, cancel := context.WithTimeout(e.ctx, processTimeout)
	defer cancel()
	defer func() {
		elapsed := time.Since(exportStart).Seconds()
		if e.durationHistogram != nil {
			e.durationHistogram.Record(ctx, elapsed)
		}
		e.duration.observe(elapsed)
	}()

	start, end := lifecycleWindow(pod)
	if start.IsZero() || !start.Before(end) {
		klog.Warningf("Pod metrics export skipped for %s: empty lifecycle window (start=%v end=%v)",
			podIdentity(pod), start, end)
		return
	}
	if e.windowHistogram != nil {
		e.windowHistogram.Record(ctx, end.Sub(start).Seconds())
	}
	step := e.queryStep(end.Sub(start))
	queries := e.buildQueries(pod)
	if len(queries) == 0 {
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
		Metrics:       make([]MetricSeries, 0, len(queries)),
	}
	// Sort the export names so the document body is reproducible; map
	// iteration order would otherwise reshuffle the array on every export.
	names := make([]string, 0, len(queries))
	for name := range queries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		spec := queries[name]
		results, err := e.querier.QueryRange(ctx, spec.Expr, start, end, step)
		if err != nil {
			// IO-layer failure: abort this pod's export. Prometheus error is
			// persistent (server unreachable / auth / expression invalid), so
			// retrying other queries on the same pod won't help.
			klog.Errorf("Pod metrics query %q failed for %s/%s: %v", name, pod.Namespace, pod.Name, err)
			return
		}
		if spec.Accumulate {
			results, err = accumulate(results, step)
			if err != nil {
				// Compute-layer failure: isolate. A non-finite sample inside
				// one series doesn't invalidate the others (or the other
				// metrics). Log loud so operators know a metric is degraded.
				klog.Warningf("Pod metrics query %q accumulate failed for %s/%s, exporting empty: %v",
					name, pod.Namespace, pod.Name, err)
				doc.Metrics = append(doc.Metrics, MetricSeries{Name: name, Values: []prometheus.Sample{}})
				continue
			}
		}
		for _, result := range results {
			doc.Metrics = append(doc.Metrics, MetricSeries{Name: name, Metric: result.Metric, Values: result.Values})
		}
	}
	if err := e.sink.PublishPodMetrics(ctx, doc); err != nil {
		// IO-layer failure: abort. The write is a single request that either
		// lands whole or not at all, and every publish inserts a new document,
		// so a later retry adds a complete record instead of patching this one.
		klog.Errorf("Failed to publish pod metrics for %s/%s: %v", pod.Namespace, pod.Name, err)
		return
	}
}

// queryStep returns the range-query step for one lifecycle window. A window
// shorter than stepThreshold is sampled every shortWindowStep so short-lived
// pods still produce several points; longer windows use the configured step
// to bound the number of samples per query.
func (e *Exporter) queryStep(window time.Duration) time.Duration {
	if window < e.stepThreshold {
		return shortWindowStep
	}
	return e.step
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
