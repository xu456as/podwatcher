package informer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"podwatcher/internal/handler"
	"podwatcher/internal/store"
	"podwatcher/pkg/k8s"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// errResourceVersionGone signals that the persisted resourceVersion is too old
// and the watcher must fall back to a full relist.
var errResourceVersionGone = errors.New("resource version gone, relisting")

// PodInformerManager lists and watches pods. It persists the last processed
// resourceVersion so that a short restart can resume the watch from that point
// and replay pod lifecycle events missed while the process was down, instead
// of only seeing pods that still exist after restart. When the persisted
// version is too old (HTTP 410 Gone) it falls back to a full relist.
type PodInformerManager struct {
	client        *k8s.Client
	handler       *handler.PodEventHandler
	store         cache.Store
	resume        store.ResumeStore
	namespace     string // empty means all namespaces
	flushInterval time.Duration

	mu     sync.Mutex
	lastRV string
	dirty  bool
	synced atomic.Bool

	stopCh chan struct{}
}

// Option customizes a PodInformerManager at construction.
type Option func(*PodInformerManager)

// WithFlushInterval sets how often the resourceVersion is checkpointed
// (default 10s). Must stay well below the cluster watch-history retention
// window, otherwise an unavailable checkpoint store can make restarts fall
// back to a full relist.
func WithFlushInterval(d time.Duration) Option {
	return func(m *PodInformerManager) { m.flushInterval = d }
}

// NewPodInformerManager creates a pod watcher across all namespaces. resume
// persists the watch resourceVersion so restarts can replay missed pods; pass
// a nop store to disable resume.
func NewPodInformerManager(k8sClient *k8s.Client, eventHandler *handler.PodEventHandler, resume store.ResumeStore, opts ...Option) *PodInformerManager {
	m := newPodInformerManager(k8sClient, eventHandler, resume)
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func newPodInformerManager(k8sClient *k8s.Client, eventHandler *handler.PodEventHandler, resume store.ResumeStore) *PodInformerManager {
	return &PodInformerManager{
		client:        k8sClient,
		handler:       eventHandler,
		store:         cache.NewStore(cache.MetaNamespaceKeyFunc),
		resume:        resume,
		flushInterval: 10 * time.Second,
		stopCh:        make(chan struct{}),
	}
}

// NewPodInformerManagerWithFilter creates a pod watcher limited to namespace.
func NewPodInformerManagerWithFilter(k8sClient *k8s.Client, eventHandler *handler.PodEventHandler, namespace string, resume store.ResumeStore, opts ...Option) *PodInformerManager {
	m := NewPodInformerManager(k8sClient, eventHandler, resume, opts...)
	m.namespace = namespace
	return m
}

// Start loads the persisted resourceVersion (if any), seeds the initial pod
// snapshot, and runs the watch loop in the background. It returns once the
// initial snapshot has been delivered so the cache is usable.
func (m *PodInformerManager) Start() error {
	m.lastRV = m.loadRV()
	if m.lastRV != "" {
		klog.Infof("Pod watcher resuming from persisted resourceVersion %s", m.lastRV)
	}

	go m.persistLoop()
	go m.run()

	deadline := time.Now().Add(60 * time.Second)
	for {
		if m.synced.Load() {
			klog.Info("Pod watcher initial sync completed")
			return nil
		}
		select {
		case <-m.stopCh:
			return fmt.Errorf("pod watcher stopped before initial sync")
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for pod watcher initial sync")
		}
	}
}

func (m *PodInformerManager) Stop() {
	close(m.stopCh)
	m.saveRV()
	klog.Info("Pod watcher stopped")
}

func (m *PodInformerManager) run() {
	backoff := time.Second
	for {
		select {
		case <-m.stopCh:
			return
		default:
		}

		err := m.listAndWatch()
		select {
		case <-m.stopCh:
			return
		default:
		}

		if errors.Is(err, errResourceVersionGone) {
			klog.Warning("Persisted resourceVersion too old, falling back to full pod relist")
			m.clearRV()
			backoff = time.Second
		} else if err != nil {
			klog.Warningf("Pod watch loop error: %v", err)
			if backoff < 30*time.Second {
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
		} else {
			backoff = time.Second
		}

		select {
		case <-m.stopCh:
			return
		case <-time.After(backoff):
		}
	}
}

func (m *PodInformerManager) listAndWatch() error {
	rv := m.currentRV()
	if rv == "" {
		if err := m.relist(!m.synced.Load()); err != nil {
			return err
		}
		m.synced.Store(true)
		rv = m.currentRV()
	} else if !m.synced.Load() {
		// Resuming from a persisted resourceVersion: seed the store and handler
		// with the current snapshot first, then replay watch events from the
		// older RV. The list RV must not become the watch position, otherwise
		// changes between the persisted RV and now would be skipped.
		if err := m.seedSnapshot(); err != nil {
			return err
		}
		m.synced.Store(true)
	}
	return m.watch(rv)
}

func (m *PodInformerManager) listPods() (*corev1.PodList, error) {
	return m.client.Clientset.CoreV1().Pods(m.namespace).List(context.TODO(), metav1.ListOptions{})
}

func (m *PodInformerManager) watchPods(rv string) (watch.Interface, error) {
	return m.client.Clientset.CoreV1().Pods(m.namespace).Watch(context.TODO(), metav1.ListOptions{
		ResourceVersion:     rv,
		AllowWatchBookmarks: true,
	})
}

// seedSnapshot lists currently live pods and feeds them to the store and
// handler as INITIAL, without advancing the watch resourceVersion. Used when
// resuming from a persisted RV so replayed events can be deduplicated against
// the fresher snapshot.
func (m *PodInformerManager) seedSnapshot() error {
	list, err := m.listPods()
	if err != nil {
		return err
	}

	objs := make([]interface{}, 0, len(list.Items))
	for i := range list.Items {
		pod := &list.Items[i]
		objs = append(objs, pod)
		m.handler.OnAdd(pod, true)
	}
	if err := m.store.Replace(objs, list.ResourceVersion); err != nil {
		return err
	}
	klog.Infof("Pod snapshot seeded: %d pods", len(list.Items))
	return nil
}

// relist lists all current pods. On the initial sync it feeds every pod to the
// handler as INITIAL and advances the RV to the list RV; later relists (after
// a 410) reconcile against the local store to avoid duplicate records.
func (m *PodInformerManager) relist(initial bool) error {
	list, err := m.listPods()
	if err != nil {
		return err
	}

	if initial {
		objs := make([]interface{}, 0, len(list.Items))
		for i := range list.Items {
			pod := &list.Items[i]
			objs = append(objs, pod)
			m.handler.OnAdd(pod, true)
		}
		if err := m.store.Replace(objs, list.ResourceVersion); err != nil {
			return err
		}
	} else {
		m.reconcile(list.Items)
	}

	m.setRV(list.ResourceVersion)
	klog.Infof("Pod relist complete: %d pods (rv=%s)", len(list.Items), list.ResourceVersion)
	return nil
}

// reconcile merges a relisted snapshot into the store and handler, emitting
// only the differences (new/updated/removed pods).
func (m *PodInformerManager) reconcile(pods []corev1.Pod) {
	listed := make(map[string]*corev1.Pod, len(pods))
	for i := range pods {
		pod := &pods[i]
		key, err := cache.MetaNamespaceKeyFunc(pod)
		if err != nil {
			continue
		}
		listed[key] = pod
	}

	for _, pod := range listed {
		existing, found, err := m.store.Get(pod)
		if err != nil {
			continue
		}
		if !found {
			m.handler.OnAdd(pod, false)
			_ = m.store.Add(pod)
			continue
		}
		old, ok := existing.(*corev1.Pod)
		if !ok || rvAtLeast(old.ResourceVersion, pod.ResourceVersion) {
			continue
		}
		m.handler.OnUpdate(old, pod)
		_ = m.store.Update(pod)
	}

	for _, obj := range m.store.List() {
		old, ok := obj.(*corev1.Pod)
		if !ok {
			continue
		}
		key, err := cache.MetaNamespaceKeyFunc(old)
		if err != nil {
			continue
		}
		if _, stillPresent := listed[key]; !stillPresent {
			m.handler.OnDelete(old)
			_ = m.store.Delete(old)
		}
	}
}

func (m *PodInformerManager) watch(rv string) error {
	w, err := m.watchPods(rv)
	if err != nil {
		if apierrors.IsGone(err) {
			return errResourceVersionGone
		}
		return err
	}
	defer w.Stop()

	klog.Infof("Pod watch started (rv=%s)", rv)

	for {
		select {
		case <-m.stopCh:
			return nil
		case e, ok := <-w.ResultChan():
			if !ok {
				// Channel closed; reconnect from the last known resourceVersion.
				return nil
			}
			switch e.Type {
			case watch.Bookmark:
				if pod, ok := e.Object.(*corev1.Pod); ok {
					m.setRV(pod.ResourceVersion)
				}
			case watch.Added:
				m.onAdded(e.Object)
			case watch.Modified:
				m.onModified(e.Object)
			case watch.Deleted:
				m.onDeleted(e.Object)
			case watch.Error:
				if isGoneStatus(e.Object) {
					return errResourceVersionGone
				}
				klog.Warningf("Pod watch error event: %v", e.Object)
			}
		}
	}
}

func (m *PodInformerManager) onAdded(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	if existing, found, _ := m.store.Get(pod); found {
		if old, ok := existing.(*corev1.Pod); ok {
			// Replay can deliver an older version than the snapshot seeded at
			// startup; never let stale state overwrite fresher state.
			if rvAtLeast(old.ResourceVersion, pod.ResourceVersion) {
				m.setRV(pod.ResourceVersion)
				return
			}
			m.handler.OnUpdate(old, pod)
			_ = m.store.Update(pod)
			m.setRV(pod.ResourceVersion)
			return
		}
	}

	m.handler.OnAdd(pod, false)
	_ = m.store.Add(pod)
	m.setRV(pod.ResourceVersion)
}

func (m *PodInformerManager) onModified(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	existing, found, _ := m.store.Get(pod)
	if found {
		if old, ok := existing.(*corev1.Pod); ok && rvAtLeast(old.ResourceVersion, pod.ResourceVersion) {
			m.setRV(pod.ResourceVersion)
			return
		}
		if old, ok := existing.(*corev1.Pod); ok {
			m.handler.OnUpdate(old, pod)
		} else {
			m.handler.OnAdd(pod, false)
		}
	} else {
		// Modified for a pod never seen (e.g. its Added event was compacted
		// away): treat it as a new discovery.
		m.handler.OnAdd(pod, false)
	}

	_ = m.store.Update(pod)
	m.setRV(pod.ResourceVersion)
}

func (m *PodInformerManager) onDeleted(obj interface{}) {
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

	// Always dispatch, even when the pod is not in the store: a pod created
	// and deleted while the watcher was down is missing from the snapshot, but
	// the deleted object still carries its labels (e.g. applicationId).
	m.handler.OnDelete(pod)
	_ = m.store.Delete(pod)
	if pod.ResourceVersion != "" {
		m.setRV(pod.ResourceVersion)
	}
}

// LookupPod returns a pod from the local store by namespace/name.
func (m *PodInformerManager) LookupPod(namespace, name string) (*corev1.Pod, bool) {
	obj, found, err := m.store.GetByKey(fmt.Sprintf("%s/%s", namespace, name))
	if err != nil || !found {
		return nil, false
	}
	pod, ok := obj.(*corev1.Pod)
	return pod, ok
}

func (m *PodInformerManager) GetStore() cache.Store {
	return m.store
}

func (m *PodInformerManager) HasSynced() bool {
	return m.synced.Load()
}

func (m *PodInformerManager) currentRV() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastRV
}

func (m *PodInformerManager) setRV(rv string) {
	if rv == "" {
		return
	}
	m.mu.Lock()
	if rv != m.lastRV {
		m.lastRV = rv
		m.dirty = true
	}
	m.mu.Unlock()
}

// clearRV drops the persisted resourceVersion so the next listAndWatch falls
// back to a full relist (e.g. after a 410 Gone). The empty RV is never written
// to disk; the relist populates a fresh RV that overwrites the stale state.
func (m *PodInformerManager) clearRV() {
	m.mu.Lock()
	m.lastRV = ""
	m.dirty = false
	m.mu.Unlock()
}

// persistLoop flushes the resourceVersion to the resume store periodically so
// a hard crash only loses one flush interval of watch progress.
func (m *PodInformerManager) persistLoop() {
	interval := m.flushInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.saveRV()
		}
	}
}

func (m *PodInformerManager) loadRV() string {
	if m.resume == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rv, found, err := m.resume.Load(ctx)
	if err != nil {
		klog.Warningf("Failed to load resume checkpoint, falling back to full relist: %v", err)
		return ""
	}
	if !found {
		return ""
	}
	return rv
}

func (m *PodInformerManager) saveRV() {
	if m.resume == nil {
		return
	}

	m.mu.Lock()
	rv := m.lastRV
	dirty := m.dirty
	m.dirty = false
	m.mu.Unlock()

	if !dirty || rv == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.resume.Save(ctx, rv); err != nil {
		klog.Warningf("Failed to persist resume checkpoint, will retry: %v", err)
		m.mu.Lock()
		m.dirty = true
		m.mu.Unlock()
	}
}

func isGoneStatus(obj interface{}) bool {
	status, ok := obj.(*metav1.Status)
	return ok && (status.Code == 410 || status.Reason == metav1.StatusReasonGone)
}

// rvAtLeast reports whether resourceVersion a is newer than or equal to b.
// Resource versions are opaque but monotonic integers in practice; fall back
// to string equality when they cannot be parsed.
func rvAtLeast(a, b string) bool {
	na, errA := strconv.ParseInt(a, 10, 64)
	nb, errB := strconv.ParseInt(b, 10, 64)
	if errA == nil && errB == nil {
		return na >= nb
	}
	return a == b
}
