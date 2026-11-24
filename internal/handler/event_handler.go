package handler

import (
	"context"
	"errors"
	"sync"
	"time"

	"podwatcher/internal/store"

	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// PodEvent is a recorded driver pod lifecycle event.
type PodEvent struct {
	Type      string            `json:"type"` // ADDED, MODIFIED, DELETED, INITIAL
	Namespace string            `json:"namespace"`
	Name      string            `json:"name"`
	Phase     string            `json:"phase"`
	Node      string            `json:"node"`
	Labels    map[string]string `json:"labels"`
	Timestamp time.Time         `json:"timestamp"`
}

// terminalEntry stores when a driver fired its terminal publish. The value is
// a timestamp so the periodic TTL sweep can drop stale entries whose OnDelete
// never arrived (watch disconnect, Kubelet crash before GC, etc.).
type terminalEntry struct {
	firedAt time.Time
}

// PodEventHandler receives pod events from the informer. It tracks Spark
// driver and executor pods (spark-role=driver|executor) into two stores:
// applications are credentials for log analysis, pod records are coordinates
// for historical resource metrics. The diagnostic event ring buffer and logs
// stay driver-only to avoid noise from frequent executor churn.
type PodEventHandler struct {
	events     []PodEvent
	eventsLock sync.RWMutex
	maxEvents  int
	stateStore *store.Store
	publisher  EventPublisher
	isDebug    bool

	// terminalNotified records driver pods whose terminal-publish suite (ES
	// DELETED event + ES application completion + Prometheus/ES pod-metrics
	// export) was already fired from an OnUpdate (Succeeded/Failed) transition.
	// Later OnDelete calls for the same pod must skip the fanout or every sink
	// (which writes a new immutable document per call) ends up with duplicates.
	//
	// Entries are normally consumed and deleted in OnDelete seconds to minutes
	// after they were written. A periodic goroutine sweeps out entries older
	// than terminalNotifiedTTL as a safety valve for cases where OnDelete
	// never arrives — so the map never grows unboundedly.
	terminalNotified sync.Map // key: pod identity string, value: terminalEntry

	// terminalNotifiedGauge reports the current terminalNotified size. It is
	// recorded at the end of each TTL sweep, reusing the Range pass that the
	// sweep already performs — no extra O(n) cost on the hot path and no
	// observable-gauge callback goroutine to manage.
	terminalNotifiedGauge otelmetric.Int64Gauge

	// stopCh terminates the periodic TTL sweep goroutine on handler shutdown.
	stopCh chan struct{}
}

const (
	defaultTerminalNotifiedTTL     = 24 * time.Hour
	defaultTerminalNotifiedSweep   = 4 * time.Hour // ~6 times a day
)

type EventPublisher interface {
	Publish(context.Context, string, *corev1.Pod, bool) error
}

type Option func(*PodEventHandler)

func WithPublisher(publisher EventPublisher) Option {
	return func(h *PodEventHandler) { h.publisher = publisher }
}

func WithDebug(enabled bool) Option {
	return func(h *PodEventHandler) { h.isDebug = enabled }
}

func NewPodEventHandler(stateStore *store.Store, opts ...Option) *PodEventHandler {
	h := &PodEventHandler{
		events:     make([]PodEvent, 0),
		maxEvents:  1000,
		stateStore: stateStore,
		stopCh:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt(h)
	}
	h.registerTerminalNotifiedGauge()
	go h.terminalSweepLoop()
	return h
}

// registerTerminalNotifiedGauge creates a synchronous Int64Gauge. Size is
// recorded inside terminalSweepLoop (same Range pass that does TTL cleanup),
// so there's no extra O(n) scan and no observable-gauge callback.
func (h *PodEventHandler) registerTerminalNotifiedGauge() {
	meter := otel.Meter("podwatcher/handler")
	gauge, err := meter.Int64Gauge(
		"podwatcher.handler.terminal_notified",
		otelmetric.WithDescription("Driver pods tracked in terminalNotified — a safety-valve set that should normally be empty or very small"),
	)
	if err != nil {
		klog.Warningf("Failed to create terminal_notified gauge: %v", err)
		return
	}
	h.terminalNotifiedGauge = gauge
}

// Stop terminates the periodic TTL sweep. Safe to call once; subsequent calls
// are no-ops.
func (h *PodEventHandler) Stop() {
	select {
	case <-h.stopCh:
	default:
		close(h.stopCh)
	}
}

// terminalSweepLoop periodically drops terminalNotified entries older than
// terminalNotifiedTTL and records the current map size into the OTel gauge.
// The size is collected inside the same Range pass that does cleanup, so the
// sweep's one O(n) scan is shared between both concerns — no extra Range on
// the hot path and no observable-gauge callback goroutine.
func (h *PodEventHandler) terminalSweepLoop() {
	ticker := time.NewTicker(defaultTerminalNotifiedSweep)
	defer ticker.Stop()
	ctx := context.Background()
	for {
		select {
		case <-h.stopCh:
			return
		case now := <-ticker.C:
			cutoff := now.Add(-defaultTerminalNotifiedTTL)
			evicted := 0
			remaining := int64(0)
			h.terminalNotified.Range(func(key, value interface{}) bool {
				entry, ok := value.(terminalEntry)
				if !ok || entry.firedAt.Before(cutoff) {
					h.terminalNotified.Delete(key)
					evicted++
				} else {
					remaining++
				}
				return true
			})
			if h.terminalNotifiedGauge != nil {
				h.terminalNotifiedGauge.Record(ctx, remaining)
			}
			if evicted > 0 {
				klog.Infof("terminalNotified sweep: evicted %d entries older than %s (remaining=%d)",
					evicted, defaultTerminalNotifiedTTL, remaining)
			} else if remaining > 0 {
				klog.V(4).Infof("terminalNotified sweep: no evictions, %d entries remain", remaining)
			}
		}
	}
}

func (h *PodEventHandler) OnAdd(obj interface{}, isInInitialList bool) {
	pod, ok := podFromObject(obj)
	if !ok {
		klog.Error("Failed to convert object to Pod")
		return
	}
	role := store.SparkRole(pod)
	if role == "" {
		return
	}

	// Startup snapshot/replay seeding is a bootstrap: the stores record the
	// current state without advancing incremental-feed cursors.
	if isInInitialList {
		h.stateStore.BootstrapPod(pod)
	} else {
		h.stateStore.UpsertPod(pod, false)
	}
	h.publish("ADDED", pod, isInInitialList)
	h.publishApplication(pod)

	if role != "driver" {
		return
	}

	eventType := "ADDED"
	if isInInitialList {
		eventType = "INITIAL"
	}

	event := PodEvent{
		Type:      eventType,
		Namespace: pod.Namespace,
		Name:      pod.Name,
		Phase:     string(pod.Status.Phase),
		Node:      pod.Spec.NodeName,
		Labels:    pod.Labels,
		Timestamp: time.Now(),
	}

	h.addEvent(event)
	klog.Infof("[POD %s] %s/%s phase=%s node=%s applicationId=%s labels=%v",
		eventType, pod.Namespace, pod.Name, event.Phase, event.Node, store.AppIDFromPod(pod), pod.Labels)
}

func (h *PodEventHandler) OnUpdate(oldObj, newObj interface{}) {
	oldPod, ok1 := oldObj.(*corev1.Pod)
	newPod, ok2 := newObj.(*corev1.Pod)
	if !ok1 || !ok2 {
		klog.Error("Failed to convert object to Pod")
		return
	}
	role := store.SparkRole(newPod)
	if role == "" {
		return
	}

	// Keep the state current on every tracked pod change.
	h.stateStore.UpsertPod(newPod, false)

	isTerminal := newPod.Status.Phase == corev1.PodSucceeded || newPod.Status.Phase == corev1.PodFailed
	phaseChanged := oldPod.Status.Phase != newPod.Status.Phase

	// Driver-only: a driver reaching Succeeded/Failed has stopped consuming
	// cluster resources, so this is the earliest safe moment to fire the
	// terminal fanout that OnDelete normally owns — ES "DELETED", ES
	// application completion, and Prometheus/ES pod-metrics. All three
	// publishes are grouped here (and nowhere else in OnUpdate) so that
	// failed/succeeded pods receiving further OnUpdate events (annotation
	// changes, deletionTimestamp, etc.) do not re-issue any of them; FinishedAt
	// is only written once by the store and the terminalNotified map keeps
	// OnDelete from firing its own fanout later.
	if role == "driver" && isTerminal && phaseChanged {
		now := time.Now()
		key := podKey(newPod)
		if _, loaded := h.terminalNotified.LoadOrStore(key, terminalEntry{firedAt: now}); !loaded {
			h.publish("DELETED", newPod, false)
			h.publishApplication(newPod)
			h.publishPodMetrics(newPod)
			klog.Infof("[POD TERMINAL] %s/%s phase=%s applicationId=%s",
				newPod.Namespace, newPod.Name, newPod.Status.Phase, store.AppIDFromPod(newPod))
		}
	}

	// The diagnostic stream records a driver lifecycle event only on the
	// transition to a terminal phase (Succeeded/Failed) to avoid noise from
	// intermediate updates.
	if role != "driver" || !isTerminal || !phaseChanged {
		return
	}

	event := PodEvent{
		Type:      "MODIFIED",
		Namespace: newPod.Namespace,
		Name:      newPod.Name,
		Phase:     string(newPod.Status.Phase),
		Node:      newPod.Spec.NodeName,
		Labels:    newPod.Labels,
		Timestamp: time.Now(),
	}
	h.addEvent(event)
	klog.Infof("[POD MODIFIED] %s/%s phase=%s -> %s applicationId=%s labels=%v",
		newPod.Namespace, newPod.Name, oldPod.Status.Phase, newPod.Status.Phase, store.AppIDFromPod(newPod), newPod.Labels)
}

func (h *PodEventHandler) OnDelete(obj interface{}) {
	pod, ok := podFromObject(obj)
	if !ok {
		klog.Error("Failed to convert object to Pod or tombstone")
		return
	}
	role := store.SparkRole(pod)
	if role == "" {
		return
	}

	h.stateStore.UpsertPod(pod, true)

	// Driver-only idempotency: if this driver already fired its terminal
	// fanout from an earlier OnUpdate (Succeeded/Failed transition), skip
	// and consume the entry. Executors don't participate in the advance-publish
	// path so they fall through to the normal three-publish call.
	key := podKey(pod)
	if _, loaded := h.terminalNotified.Load(key); loaded && role == "driver" {
		h.terminalNotified.Delete(key)
		klog.Infof("[POD DELETED] %s/%s phase=%s (terminal fanout already sent, skipping)",
			pod.Namespace, pod.Name, pod.Status.Phase)
	} else {
		h.publish("DELETED", pod, false)
		h.publishApplication(pod)
		h.publishPodMetrics(pod)
	}

	if role != "driver" {
		return
	}

	event := PodEvent{
		Type:      "DELETED",
		Namespace: pod.Namespace,
		Name:      pod.Name,
		Phase:     string(pod.Status.Phase),
		Node:      pod.Spec.NodeName,
		Labels:    pod.Labels,
		Timestamp: time.Now(),
	}

	h.addEvent(event)
	klog.Infof("[POD DELETED] %s/%s phase=%s applicationId=%s labels=%v",
		pod.Namespace, pod.Name, event.Phase, store.AppIDFromPod(pod), pod.Labels)
}

func (h *PodEventHandler) publish(eventType string, pod *corev1.Pod, initial bool) {
	if h.publisher == nil {
		return
	}
	if h.isDebug {
		klog.Infof("[DEBUG] Elasticsearch publishing pod %s %s/%s initial=%t", eventType, pod.Namespace, pod.Name, initial)
	}
	if err := h.publisher.Publish(context.Background(), eventType, pod, initial); err != nil {
		klog.Errorf("Failed to publish pod %s %s/%s to Elasticsearch: %v", eventType, pod.Namespace, pod.Name, err)
		h.logPublishPayload(err)
	} else if h.isDebug {
		klog.Infof("[DEBUG] Elasticsearch published pod %s %s/%s", eventType, pod.Namespace, pod.Name)
	}
}

type ApplicationPublisher interface {
	PublishApplication(context.Context, store.ApplicationView) error
}

// PodMetricsPublisher optionally exports a deleted Spark pod's Prometheus
// metrics. Like ApplicationPublisher it is probed at runtime; implementations
// must accept quickly and run the slow Prometheus/Elasticsearch work
// asynchronously so informer callbacks are never blocked.
type PodMetricsPublisher interface {
	PublishPodMetrics(context.Context, *corev1.Pod) error
}

func (h *PodEventHandler) publishApplication(pod *corev1.Pod) {
	if store.SparkRole(pod) != "driver" {
		return
	}
	publisher, ok := h.publisher.(ApplicationPublisher)
	if !ok {
		return
	}
	app, found := h.stateStore.GetApplication(store.AppIDFromPod(pod))
	if !found || app.FinishedAt == nil {
		return
	}
	if h.isDebug {
		klog.Infof("[DEBUG] Elasticsearch publishing completed application %s", app.ApplicationID)
	}
	if err := publisher.PublishApplication(context.Background(), app); err != nil {
		klog.Errorf("Failed to publish completed application %s to Elasticsearch: %v", app.ApplicationID, err)
		h.logPublishPayload(err)
	} else if h.isDebug {
		klog.Infof("[DEBUG] Elasticsearch published completed application %s", app.ApplicationID)
	}
}

// publishPodMetrics hands a deleted Spark pod to the optional metrics
// exporter. Failures here must not affect the state stores or the
// diagnostic stream; the exporter owns retries and slow I/O.
func (h *PodEventHandler) publishPodMetrics(pod *corev1.Pod) {
	publisher, ok := h.publisher.(PodMetricsPublisher)
	if !ok {
		return
	}
	if h.isDebug {
		klog.Infof("[DEBUG] enqueueing pod metrics export %s/%s", pod.Namespace, pod.Name)
	}
	if err := publisher.PublishPodMetrics(context.Background(), pod); err != nil {
		klog.Errorf("Failed to enqueue pod metrics export %s/%s: %v", pod.Namespace, pod.Name, err)
	}
}

func (h *PodEventHandler) logPublishPayload(err error) {
	if !h.isDebug {
		return
	}
	var detail interface{ DebugPayload() string }
	if errors.As(err, &detail) {
		klog.Infof("[DEBUG] Elasticsearch failed document JSON: %s", detail.DebugPayload())
	}
}

// podFromObject extracts a Pod from an event object, unwrapping a
// DeletedFinalStateUnknown tombstone when the watch cache missed the deletion.
func podFromObject(obj interface{}) (*corev1.Pod, bool) {
	if pod, ok := obj.(*corev1.Pod); ok {
		return pod, true
	}
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}
	pod, ok := tombstone.Obj.(*corev1.Pod)
	return pod, ok
}

// podKey returns the identity shared by Elasticsearch, the metrics exporter
// and terminalNotified. UID first, falling back to namespace/name.
func podKey(pod *corev1.Pod) string {
	if pod.UID != "" {
		return string(pod.UID)
	}
	return pod.Namespace + "/" + pod.Name
}

func (h *PodEventHandler) addEvent(event PodEvent) {
	h.eventsLock.Lock()
	defer h.eventsLock.Unlock()

	h.events = append([]PodEvent{event}, h.events...)

	if len(h.events) > h.maxEvents {
		h.events = h.events[:h.maxEvents]
	}
}

func (h *PodEventHandler) GetEvents(limit int) []PodEvent {
	h.eventsLock.RLock()
	defer h.eventsLock.RUnlock()

	if limit > 0 && limit < len(h.events) {
		return h.events[:limit]
	}
	return h.events
}

func (h *PodEventHandler) GetStats() map[string]int {
	h.eventsLock.RLock()
	defer h.eventsLock.RUnlock()

	stats := make(map[string]int)
	for _, event := range h.events {
		stats[event.Type]++
	}
	return stats
}

// GetApplications returns one changedAt-ordered page of application
// credentials changed strictly after since, optionally filtered by exact
// applicationId and a set of statuses.
func (h *PodEventHandler) GetApplications(since time.Time, limit int, appID string, statuses []string) ([]store.ApplicationView, bool) {
	return h.stateStore.GetChanges(since, limit, appID, statuses)
}

// GetApplication returns one application credential by id.
func (h *PodEventHandler) GetApplication(appID string) (store.ApplicationView, bool) {
	return h.stateStore.GetApplication(appID)
}

// GetPodRecords returns historical driver/executor pod coordinates matching
// the filter.
func (h *PodEventHandler) GetPodRecords(filter store.PodRecordFilter) []store.PodRecord {
	return h.stateStore.GetPodRecords(filter)
}
