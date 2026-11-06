package handler

import (
	"context"
	"errors"
	"sync"
	"time"

	"podwatcher/internal/store"

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
}

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
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
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
	h.publishApplication(newPod)

	// The diagnostic stream records a driver lifecycle event only on the
	// transition to a terminal phase (Succeeded/Failed) to avoid noise from
	// intermediate updates.
	if role != "driver" || oldPod.Status.Phase == newPod.Status.Phase ||
		(newPod.Status.Phase != corev1.PodSucceeded && newPod.Status.Phase != corev1.PodFailed) {
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
	h.publish("DELETED", pod, false)
	h.publishApplication(pod)

	// The pod's lifecycle is over: hand it to the optional async metrics
	// exporter for its final resource-usage record. Both drivers and
	// executors are exported.
	h.publishPodMetrics(pod)

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
