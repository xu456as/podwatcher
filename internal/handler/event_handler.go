package handler

import (
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

type PodEvent struct {
	Type      string            `json:"type"` // ADDED, MODIFIED, DELETED
	Namespace string            `json:"namespace"`
	Name      string            `json:"name"`
	Phase     string            `json:"phase"`
	Node      string            `json:"node"`
	Labels    map[string]string `json:"labels"`
	Timestamp time.Time         `json:"timestamp"`
}

// SparkPodInfo 记录单个 Spark Pod 信息及其状态
type SparkPodInfo struct {
	Name             string            `json:"name"`
	Namespace        string            `json:"namespace"`
	ApplicationID    string            `json:"applicationId"`
	SparkAppSelector string            `json:"sparkAppSelector"`
	Phase            string            `json:"phase"`
	Status           string            `json:"status"` // running, succeeded, deleted, failed
	Node             string            `json:"node"`
	Labels           map[string]string `json:"labels"`
	UpdatedAt        time.Time         `json:"updatedAt"`
}

// SparkApplication 记录一个 Spark Application 的所有 Pod
type SparkApplication struct {
	ApplicationID    string         `json:"applicationId"`
	SparkAppSelector string         `json:"sparkAppSelector"`
	Pods             []SparkPodInfo `json:"pods"`
	FirstSeenAt      time.Time      `json:"firstSeenAt"`
	LastUpdatedAt    time.Time      `json:"lastUpdatedAt"`
	PodCount         int            `json:"podCount"`
}

// SparkAppCollector 收集所有 Spark Pod，按 application 分组，跟踪状态变化
type SparkAppCollector struct {
	applications  map[string]*SparkApplication
	lock          sync.RWMutex
	maxApps       int
	maxPodsPerApp int
}

func NewSparkAppCollector(maxApps, maxPodsPerApp int) *SparkAppCollector {
	return &SparkAppCollector{
		applications:  make(map[string]*SparkApplication),
		maxApps:       maxApps,
		maxPodsPerApp: maxPodsPerApp,
	}
}

func (c *SparkAppCollector) isSparkPod(pod *corev1.Pod) bool {
	if pod.Labels == nil {
		return false
	}
	_, hasAppID := pod.Labels["applicationId"]
	_, hasSelector := pod.Labels["spark-app-selector"]
	return hasAppID || hasSelector
}

func (c *SparkAppCollector) getApplicationID(pod *corev1.Pod) string {
	if appID, ok := pod.Labels["applicationId"]; ok && appID != "" {
		return appID
	}
	if selector, ok := pod.Labels["spark-app-selector"]; ok && selector != "" {
		return selector
	}
	return ""
}

// podStatus 根据 phase 和是否 deleted 推断状态
func podStatus(pod *corev1.Pod, deleted bool) string {
	if deleted {
		return "deleted"
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		return "succeeded"
	case corev1.PodFailed:
		return "failed"
	case corev1.PodRunning:
		return "running"
	case corev1.PodPending:
		return "pending"
	default:
		return string(pod.Status.Phase)
	}
}

func (c *SparkAppCollector) UpsertPod(pod *corev1.Pod, deleted bool) {
	appID := c.getApplicationID(pod)
	if appID == "" {
		return
	}

	now := time.Now()
	status := podStatus(pod, deleted)

	sparkPod := SparkPodInfo{
		Name:             pod.Name,
		Namespace:        pod.Namespace,
		ApplicationID:    pod.Labels["applicationId"],
		SparkAppSelector: pod.Labels["spark-app-selector"],
		Phase:            string(pod.Status.Phase),
		Status:           status,
		Node:             pod.Spec.NodeName,
		Labels:           pod.Labels,
		UpdatedAt:        now,
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	app, exists := c.applications[appID]
	if !exists {
		app = &SparkApplication{
			ApplicationID:    appID,
			SparkAppSelector: sparkPod.SparkAppSelector,
			Pods:             make([]SparkPodInfo, 0),
			FirstSeenAt:      now,
			LastUpdatedAt:    now,
		}
		c.applications[appID] = app
	}

	// 更新已存在的 pod 或追加新 pod
	found := false
	for i, p := range app.Pods {
		if p.Name == pod.Name {
			app.Pods[i] = sparkPod
			found = true
			break
		}
	}
	if !found {
		app.Pods = append(app.Pods, sparkPod)
	}
	app.PodCount = len(app.Pods)
	if now.After(app.LastUpdatedAt) {
		app.LastUpdatedAt = now
	}

	// 每个 application 限制 pod 数量
	if len(app.Pods) > c.maxPodsPerApp {
		app.Pods = app.Pods[len(app.Pods)-c.maxPodsPerApp:]
		app.PodCount = len(app.Pods)
	}

	// 超过上限淘汰最早的
	if len(c.applications) > c.maxApps {
		var oldestID string
		var oldestTime time.Time
		for id, a := range c.applications {
			if oldestID == "" || a.FirstSeenAt.Before(oldestTime) {
				oldestID = id
				oldestTime = a.FirstSeenAt
			}
		}
		delete(c.applications, oldestID)
	}

	klog.Infof("[SPARK APP] Upserted pod %s/%s status=%s for application %s", pod.Namespace, pod.Name, status, appID)
}

func (c *SparkAppCollector) GetByTimeRange(startTime, endTime time.Time) []SparkApplication {
	c.lock.Lock()
	defer c.lock.Unlock()

	// 淘汰两周前的数据
	cutoff := time.Now().Add(-14 * 24 * time.Hour)
	for id, app := range c.applications {
		if app.LastUpdatedAt.Before(cutoff) {
			delete(c.applications, id)
		}
	}

	var result []SparkApplication
	for _, app := range c.applications {
		matched := false
		for _, pod := range app.Pods {
			if (pod.UpdatedAt.Equal(startTime) || pod.UpdatedAt.After(startTime)) &&
				(pod.UpdatedAt.Equal(endTime) || pod.UpdatedAt.Before(endTime)) {
				matched = true
				break
			}
		}
		if matched {
			result = append(result, *app)
		}
	}
	return result
}

func (c *SparkAppCollector) GetAll() []SparkApplication {
	c.lock.RLock()
	defer c.lock.RUnlock()

	var result []SparkApplication
	for _, app := range c.applications {
		result = append(result, *app)
	}
	return result
}

type PodEventHandler struct {
	events         []PodEvent
	eventsLock     sync.RWMutex
	maxEvents      int
	sparkCollector *SparkAppCollector
}

func NewPodEventHandler() *PodEventHandler {
	return &PodEventHandler{
		events:         make([]PodEvent, 0),
		maxEvents:      1000,
		sparkCollector: NewSparkAppCollector(1000, 100),
	}
}

func (h *PodEventHandler) OnAdd(obj interface{}, isInInitialList bool) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		klog.Error("Failed to convert object to Pod")
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
	klog.Infof("[POD %s] %s/%s - Phase: %s - Node: %s - Labels: %v - Annotations: %v",
		eventType, pod.Namespace, pod.Name, pod.Status.Phase, pod.Spec.NodeName, pod.Labels, pod.Annotations)

	if h.sparkCollector.isSparkPod(pod) {
		h.sparkCollector.UpsertPod(pod, false)
	}
}

func (h *PodEventHandler) OnUpdate(oldObj, newObj interface{}) {
	oldPod, ok1 := oldObj.(*corev1.Pod)
	newPod, ok2 := newObj.(*corev1.Pod)

	if !ok1 || !ok2 {
		klog.Error("Failed to convert object to Pod")
		return
	}

	if oldPod.Status.Phase != newPod.Status.Phase {
		// 只保留变到 Succeeded
		if newPod.Status.Phase != corev1.PodSucceeded {
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
		klog.Infof("[POD MODIFIED] %s/%s: %s -> %s - Labels: %v - Annotations: %v",
			newPod.Namespace, newPod.Name, oldPod.Status.Phase, newPod.Status.Phase, newPod.Labels, newPod.Annotations)
	}

	// spark pod 状态变更时更新
	if h.sparkCollector.isSparkPod(newPod) {
		h.sparkCollector.UpsertPod(newPod, false)
	}
}

func (h *PodEventHandler) OnDelete(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Error("Failed to convert object to Pod or tombstone")
			return
		}
		pod, ok = tombstone.Obj.(*corev1.Pod)
		if !ok {
			klog.Error("Failed to convert tombstone object to Pod")
			return
		}
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
	klog.Infof("[POD DELETED] %s/%s - Labels: %v - Annotations: %v", pod.Namespace, pod.Name, pod.Labels, pod.Annotations)

	if h.sparkCollector.isSparkPod(pod) {
		h.sparkCollector.UpsertPod(pod, true)
	}
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

func (h *PodEventHandler) GetSparkApps(startTime, endTime time.Time) []SparkApplication {
	return h.sparkCollector.GetByTimeRange(startTime, endTime)
}

func (h *PodEventHandler) GetAllSparkApps() []SparkApplication {
	return h.sparkCollector.GetAll()
}
