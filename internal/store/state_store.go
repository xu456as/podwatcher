package store

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// durationBuckets cover sub-millisecond up to 10s latencies of in-memory
// store operations, dense enough at the low end for p95/p99 to be meaningful.
var durationBuckets = []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

var (
	durationOnce  sync.Once
	durationHist  otelmetric.Float64Histogram
	durationGauge otelmetric.Float64Gauge
)

// measureFunc starts a wall-clock timer for a Store method and returns the
// end function recording it into two instruments (attribute function=<name>):
//   - podwatcher.store.function.duration histogram: bucketed durations for
//     histogram_quantile (p80/p90/p99) and _sum/_count (mean) on the backend;
//   - podwatcher.store.function.duration.last gauge: last observed duration,
//     so backends can approximate the max with max_over_time.
//
// The instruments are created lazily on first use: a package-level creation
// would bind to the no-op global MeterProvider before main installs the real
// one.
func measureFunc(function string) func() {
	start := time.Now()
	return func() {
		elapsed := time.Since(start).Seconds()
		durationOnce.Do(func() {
			meter := otel.Meter("podwatcher/store")
			h, err := meter.Float64Histogram(
				"podwatcher.store.function.duration",
				otelmetric.WithUnit("s"),
				otelmetric.WithExplicitBucketBoundaries(durationBuckets...),
			)
			if err != nil {
				klog.Warningf("Failed to create store duration histogram: %v", err)
				return
			}
			g, err := meter.Float64Gauge(
				"podwatcher.store.function.duration.last",
				otelmetric.WithUnit("s"),
			)
			if err != nil {
				klog.Warningf("Failed to create store duration gauge: %v", err)
			}
			durationHist = h
			durationGauge = g
		})
		if durationHist == nil {
			return
		}
		attrs := otelmetric.WithAttributes(attribute.String("function", function))
		durationHist.Record(context.Background(), elapsed, attrs)
		if durationGauge != nil {
			durationGauge.Record(context.Background(), elapsed, attrs)
		}
	}
}

// ApplicationRecord is the log-analysis credential for one Spark
// application. Downstream uses applicationId to fetch the event log from the
// Spark history server / S3. The application lifecycle is driven by its
// single driver pod; executor pods only refresh identity and timestamps.
type ApplicationRecord struct {
	ApplicationID    string     `json:"applicationId"`
	Cluster          string     `json:"cluster,omitempty"`
	SparkAppName     string     `json:"sparkAppName,omitempty"`
	SparkAppSelector string     `json:"sparkAppSelector,omitempty"`
	Namespace        string     `json:"namespace"`
	DriverPodName    string     `json:"driverPodName,omitempty"`
	Status           string     `json:"status"` // unknown, pending, running, succeeded, failed, deleted
	ChangedAt        time.Time  `json:"changedAt"`
	CreatedAt        time.Time  `json:"createdAt"`
	FirstSeenAt      time.Time  `json:"firstSeenAt"`
	LastUpdatedAt    time.Time  `json:"lastUpdatedAt"`
	FinishedAt       *time.Time `json:"finishedAt,omitempty"`
	DeletedAt        *time.Time `json:"deletedAt,omitempty"`
}

// PodRecord is the monitoring-system coordinate for one Spark pod (driver or
// executor). It outlives the pod itself: after deletion the Kubernetes API no
// longer returns the pod, but name/node and the container time window are
// still needed to look up historical CPU/memory/disk metrics.
type PodRecord struct {
	UID                 string            `json:"uid"`
	Cluster             string            `json:"cluster,omitempty"`
	ApplicationID       string            `json:"applicationId"`
	Namespace           string            `json:"namespace"`
	Name                string            `json:"name"`
	Node                string            `json:"node"`
	Role                string            `json:"role"` // driver, executor
	ExecutorID          string            `json:"executorId,omitempty"`
	Labels              map[string]string `json:"labels,omitempty"`
	Phase               string            `json:"phase"`
	Status              string            `json:"status"` // pending, running, succeeded, failed, deleted
	CreatedAt           time.Time         `json:"createdAt"`
	ContainerStartedAt  *time.Time        `json:"containerStartedAt,omitempty"`
	ContainerFinishedAt *time.Time        `json:"containerFinishedAt,omitempty"`
	DeletionTimestamp   *time.Time        `json:"deletionTimestamp,omitempty"`
	FirstSeenAt         time.Time         `json:"firstSeenAt"`
	LastUpdatedAt       time.Time         `json:"lastUpdatedAt"`
	DeletedAt           *time.Time        `json:"deletedAt,omitempty"`
}

// ApplicationView is an application credential joined with its driver pod's
// current phase and labels so downstream needs no per-application follow-up
// query. The driver fields stay empty until a driver pod has been observed.
type ApplicationView struct {
	ApplicationRecord
	DriverPhase  string            `json:"driverPhase,omitempty"`
	DriverLabels map[string]string `json:"driverLabels,omitempty"`
}

// PodRecordFilter narrows a pod record query. A zero Limit returns all matches.
type PodRecordFilter struct {
	ApplicationID string
	Role          string
	Node          string
	Status        string
	Start         *time.Time
	End           *time.Time
	Limit         int
}

// Store keeps application credentials and driver/executor pod coordinates in
// one in-memory model and mirrors them to a single state file, so evicting an
// application and deleting its pod coordinates always commit together. When
// the limits are exceeded, deleted applications are evicted oldest-first and
// take all their pods with them; deleted executor pods are then evicted
// independently, while a live application's driver pod is never removed.
type Store struct {
	mu              sync.RWMutex
	apps            map[string]*ApplicationRecord
	pods            map[string]*PodRecord
	driverByApp     map[string]string
	cluster         string
	lastChanged     time.Time
	maxApps         int
	maxPods         int
	filePath        string
	dirty           bool
	overLimitWarned bool
	stopCh          chan struct{}
}

// Option customizes a Store at construction.
type Option func(*Store)

// WithCluster stamps cluster into every application and pod record.
func WithCluster(cluster string) Option {
	return func(s *Store) { s.cluster = cluster }
}

// NewStore loads any persisted state from filePath (if set) and starts a
// background flush loop. A non-positive limit uses the default.
func NewStore(filePath string, maxApps, maxPods int, opts ...Option) *Store {
	defer measureFunc("Store.NewStore")()
	if maxApps <= 0 {
		maxApps = 10000
	}
	if maxPods <= 0 {
		maxPods = 10000
	}
	s := &Store{
		apps:        make(map[string]*ApplicationRecord),
		pods:        make(map[string]*PodRecord),
		driverByApp: make(map[string]string),
		maxApps:     maxApps,
		maxPods:     maxPods,
		filePath:    filePath,
		stopCh:      make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.load()
	go s.persistLoop()
	return s
}

// Stop terminates the flush loop and performs a final save.
func (s *Store) Stop() {
	defer measureFunc("Store.Stop")()
	close(s.stopCh)
	s.save()
}

// UpsertPod creates or updates the records for the application owning pod.
// Only driver pods drive application status/finishedAt/deletedAt and
// changedAt; executor churn merely refreshes lastUpdatedAt and never reaches
// the incremental feed.
func (s *Store) UpsertPod(pod *corev1.Pod, deleted bool) {
	defer measureFunc("Store.UpsertPod")()
	s.upsert(pod, deleted, false)
}

// BootstrapPod seeds or refreshes records from the startup list/snapshot
// without treating the current state as a freshly observed lifecycle change.
// Fields are populated as usual, but application changedAt is anchored to the
// pod's own history and the global change cursor never advances, so a restart
// snapshot cannot flood the incremental feed; pod lastUpdatedAt is anchored to
// the container start time and deletion is never implied.
func (s *Store) BootstrapPod(pod *corev1.Pod) {
	defer measureFunc("Store.BootstrapPod")()
	s.upsert(pod, false, true)
}

func (s *Store) upsert(pod *corev1.Pod, deleted, bootstrap bool) {
	appID := AppIDFromPod(pod)
	role := SparkRole(pod)
	if appID == "" || role == "" {
		return
	}

	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	appCreated := s.upsertAppLocked(pod, appID, deleted, bootstrap, now)
	podCreated := s.upsertPodRecordLocked(pod, appID, role, deleted, bootstrap, now)
	appEvicted := s.evictAppsLocked()
	podEvicted := s.evictPodsLocked()

	if !bootstrap || appCreated || podCreated || appEvicted > 0 || podEvicted > 0 {
		s.dirty = true
	}
}

// upsertAppLocked applies the application half of an observed pod change and
// reports whether a new application record was created.
func (s *Store) upsertAppLocked(pod *corev1.Pod, appID string, deleted, bootstrap bool, now time.Time) bool {
	rec, exists := s.apps[appID]
	lifecycleChanged := false
	if !exists {
		created := pod.CreationTimestamp.Time
		if created.IsZero() {
			created = now
		}
		rec = &ApplicationRecord{
			ApplicationID: appID,
			Namespace:     pod.Namespace,
			Status:        "unknown",
			FirstSeenAt:   now,
			CreatedAt:     created,
		}
		s.apps[appID] = rec
		if !bootstrap {
			lifecycleChanged = true
		}
	}

	if created := pod.CreationTimestamp.Time; !created.IsZero() && created.Before(rec.CreatedAt) {
		rec.CreatedAt = created
	}
	if s.cluster != "" {
		rec.Cluster = s.cluster
	}
	if rec.Namespace == "" {
		rec.Namespace = pod.Namespace
	}
	if name := pod.Labels["spark-app-name"]; name != "" {
		rec.SparkAppName = name
	}
	if selector := pod.Labels["spark-app-selector"]; selector != "" {
		rec.SparkAppSelector = selector
	}

	if isDriverPod(pod) {
		rec.DriverPodName = pod.Name
		if nextStatus := podStatus(pod, deleted); rec.Status != nextStatus {
			rec.Status = nextStatus
			if !bootstrap {
				lifecycleChanged = true
			}
		}
		if deleted || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			if rec.FinishedAt == nil {
				finished := now
				if _, containerFinished := containerTimes(pod); containerFinished != nil {
					finished = *containerFinished
				}
				rec.FinishedAt = &finished
				if !bootstrap {
					lifecycleChanged = true
				}
			}
		}
		if deleted && rec.DeletedAt == nil {
			deletedAt := now
			rec.DeletedAt = &deletedAt
			if !bootstrap {
				lifecycleChanged = true
			}
		}
	}

	if bootstrap {
		// Snapshot-seeded records are anchored to real history; bootstrapping an
		// already known record (e.g. persisted state plus startup snapshot) never
		// moves its change cursor.
		if !exists {
			rec.ChangedAt = bootstrapChangedAt(pod, rec, now)
		}
	} else if lifecycleChanged {
		// Stamp a strictly increasing change time, bumped by at least 1ms so that
		// multiple changes inside the same millisecond never collide.
		next := now
		if !s.lastChanged.IsZero() {
			if floor := s.lastChanged.Add(time.Millisecond); next.Before(floor) {
				next = floor
			}
		}
		s.lastChanged = next
		rec.ChangedAt = next
	}

	rec.LastUpdatedAt = now
	return !exists
}

// upsertPodRecordLocked applies the pod half of an observed pod change and
// reports whether a new pod record was created.
func (s *Store) upsertPodRecordLocked(pod *corev1.Pod, appID, role string, deleted, bootstrap bool, now time.Time) bool {
	key := podKey(pod)
	rec, exists := s.pods[key]
	if !exists {
		created := pod.CreationTimestamp.Time
		if created.IsZero() {
			created = now
		}
		rec = &PodRecord{
			UID:           key,
			ApplicationID: appID,
			Namespace:     pod.Namespace,
			Name:          pod.Name,
			FirstSeenAt:   now,
			CreatedAt:     created,
		}
		s.pods[key] = rec
	}

	if created := pod.CreationTimestamp.Time; !created.IsZero() && created.Before(rec.CreatedAt) {
		rec.CreatedAt = created
	}
	if s.cluster != "" {
		rec.Cluster = s.cluster
	}
	rec.ApplicationID = appID
	rec.Namespace = pod.Namespace
	rec.Name = pod.Name
	rec.Node = pod.Spec.NodeName
	rec.Role = role
	rec.ExecutorID = pod.Labels["spark-exec-id"]
	if pod.Labels != nil {
		labels := make(map[string]string, len(pod.Labels))
		for labelKey, value := range pod.Labels {
			labels[labelKey] = value
		}
		rec.Labels = labels
	} else {
		rec.Labels = nil
	}
	rec.Phase = string(pod.Status.Phase)
	rec.Status = podStatus(pod, deleted)
	if started, finished := containerTimes(pod); started != nil {
		rec.ContainerStartedAt = started
		if finished != nil {
			rec.ContainerFinishedAt = finished
		}
	}
	if pod.DeletionTimestamp != nil {
		deletionTimestamp := pod.DeletionTimestamp.Time
		rec.DeletionTimestamp = &deletionTimestamp
	}
	if bootstrap {
		rec.LastUpdatedAt = bootstrapPodTime(pod, now)
	} else {
		rec.LastUpdatedAt = now
		if deleted && rec.DeletedAt == nil {
			deletedAt := now
			rec.DeletedAt = &deletedAt
		}
	}
	if isDriverPod(pod) {
		s.driverByApp[appID] = key
	}
	return !exists
}

// bootstrapChangedAt anchors the change time of a snapshot-seeded record to
// the pod's own history: the terminal finish time when present, otherwise the
// creation timestamp, falling back to now when neither is available so the
// record can never become invisible to the incremental feed.
func bootstrapChangedAt(pod *corev1.Pod, rec *ApplicationRecord, fallback time.Time) time.Time {
	if rec.FinishedAt != nil {
		return *rec.FinishedAt
	}
	if created := pod.CreationTimestamp.Time; !created.IsZero() {
		return created
	}
	return fallback
}

// bootstrapPodTime anchors the update time of a snapshot-seeded record to the
// pod's own history: the main container start time (or pod start time via
// containerTimes), then the creation timestamp, falling back to now.
func bootstrapPodTime(pod *corev1.Pod, fallback time.Time) time.Time {
	if started, _ := containerTimes(pod); started != nil {
		return *started
	}
	if !pod.CreationTimestamp.IsZero() {
		return pod.CreationTimestamp.Time
	}
	return fallback
}

// podKey identifies a pod by UID, falling back to namespace/name for objects
// without a UID.
func podKey(pod *corev1.Pod) string {
	if pod.UID != "" {
		return string(pod.UID)
	}
	return pod.Namespace + "/" + pod.Name
}

// containerTimes resolves the main (non-init) container start/finish times,
// falling back to the pod start time when container status is unavailable.
func containerTimes(pod *corev1.Pod) (started, finished *time.Time) {
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.State.Terminated != nil {
			term := cs.State.Terminated
			if !term.StartedAt.IsZero() {
				t := term.StartedAt.Time
				started = &t
			}
			if !term.FinishedAt.IsZero() {
				t := term.FinishedAt.Time
				finished = &t
			}
			return started, finished
		}
		if cs.State.Running != nil {
			t := cs.State.Running.StartedAt.Time
			started = &t
			return started, nil
		}
	}
	if pod.Status.StartTime != nil {
		t := pod.Status.StartTime.Time
		started = &t
	}
	return started, nil
}

// evictAppsLocked removes over-limit deleted applications oldest-first and
// cascades the deletion to every pod owned by them, all in the same critical
// section. It returns the number of evicted applications.
func (s *Store) evictAppsLocked() int {
	defer measureFunc("Store.evictAppsLocked")()
	evicted := 0
	for len(s.apps) > s.maxApps {
		var oldestID string
		var oldestTime time.Time
		for id, rec := range s.apps {
			if rec.DeletedAt == nil {
				continue
			}
			ts := *rec.DeletedAt
			if oldestID == "" || ts.Before(oldestTime) {
				oldestID = id
				oldestTime = ts
			}
		}
		if oldestID == "" {
			break
		}
		s.deleteAppLocked(oldestID)
		evicted++
	}
	recordAppEvictions(evicted)
	return evicted
}

// deleteAppLocked removes one application, its driver join index entry, and
// every pod record carrying its application id.
func (s *Store) deleteAppLocked(appID string) {
	delete(s.apps, appID)
	delete(s.driverByApp, appID)
	cascaded := 0
	for key, rec := range s.pods {
		if rec.ApplicationID == appID {
			delete(s.pods, key)
			cascaded++
		}
	}
	recordPodCascadeDeletes(cascaded)
}

// evictPodsLocked removes over-limit deleted pod records oldest-first. A live
// application's driver pod is protected; executors and records whose
// application no longer exists are eligible. It returns the number removed and
// warns once per overflow episode when no reclaimable record remains.
func (s *Store) evictPodsLocked() int {
	defer measureFunc("Store.evictPodsLocked")()
	removed := 0
	if len(s.pods) <= s.maxPods {
		s.overLimitWarned = false
		return removed
	}

	type candidate struct {
		key string
		at  time.Time
	}
	candidates := make([]candidate, 0)
	for key, rec := range s.pods {
		if rec.DeletedAt == nil {
			continue
		}
		_, appExists := s.apps[rec.ApplicationID]
		if rec.Role == "driver" && appExists {
			continue
		}
		candidates = append(candidates, candidate{key: key, at: *rec.DeletedAt})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].at.Before(candidates[j].at)
	})
	for _, cand := range candidates {
		if len(s.pods) <= s.maxPods {
			break
		}
		delete(s.pods, cand.key)
		removed++
	}

	if len(s.pods) > s.maxPods {
		if !s.overLimitWarned {
			klog.Warningf("Pod store over limit (%d > %d) with no reclaimable records; retaining protected drivers", len(s.pods), s.maxPods)
			s.overLimitWarned = true
		}
	} else {
		s.overLimitWarned = false
	}
	recordPodEvictions(removed)
	return removed
}

// GetApplication returns one application joined with its driver pod fields.
func (s *Store) GetApplication(appID string) (ApplicationView, bool) {
	defer measureFunc("Store.GetApplication")()
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.apps[appID]
	if !ok {
		return ApplicationView{}, false
	}
	return s.viewLocked(*rec), true
}

// GetChanges returns records whose changedAt is strictly after since,
// optionally filtered by exact applicationId and a set of statuses, ordered by
// changedAt ascending. A positive limit pages the result (limit+1 probe): at
// most limit records are returned and hasMore reports whether another page
// exists. Evicted applications can never be returned, so a since older than
// the retained window may miss terminal states.
func (s *Store) GetChanges(since time.Time, limit int, appID string, statuses []string) (views []ApplicationView, hasMore bool) {
	defer measureFunc("Store.GetChanges")()
	statusSet := make(map[string]struct{}, len(statuses))
	for _, st := range statuses {
		if st != "" {
			statusSet[st] = struct{}{}
		}
	}
	s.mu.RLock()
	matched := make([]ApplicationRecord, 0, len(s.apps))
	for _, rec := range s.apps {
		if !rec.ChangedAt.After(since) {
			continue
		}
		if appID != "" && rec.ApplicationID != appID {
			continue
		}
		if len(statusSet) > 0 {
			if _, ok := statusSet[rec.Status]; !ok {
				continue
			}
		}
		matched = append(matched, *rec)
	}
	s.mu.RUnlock()

	sort.Slice(matched, func(i, j int) bool {
		return matched[i].ChangedAt.Before(matched[j].ChangedAt)
	})

	if limit > 0 && len(matched) > limit {
		matched = matched[:limit]
		hasMore = true
	}
	views = make([]ApplicationView, len(matched))
	s.mu.RLock()
	for i, rec := range matched {
		views[i] = s.viewLocked(rec)
	}
	s.mu.RUnlock()
	return views, hasMore
}

// viewLocked joins an application record with its driver pod fields; caller
// must hold at least a read lock.
func (s *Store) viewLocked(rec ApplicationRecord) ApplicationView {
	view := ApplicationView{ApplicationRecord: rec}
	if driverKey, ok := s.driverByApp[rec.ApplicationID]; ok {
		if driver, ok := s.pods[driverKey]; ok {
			view.DriverPhase = driver.Phase
			view.DriverLabels = driver.Labels
		}
	}
	return view
}

// GetPodRecords returns pod records matching filter, ordered by lastUpdatedAt
// descending and optionally truncated to filter.Limit.
func (s *Store) GetPodRecords(filter PodRecordFilter) []PodRecord {
	defer measureFunc("Store.GetPodRecords")()
	s.mu.RLock()
	result := make([]PodRecord, 0, len(s.pods))
	for _, rec := range s.pods {
		if filter.ApplicationID != "" && rec.ApplicationID != filter.ApplicationID {
			continue
		}
		if filter.Role != "" && rec.Role != filter.Role {
			continue
		}
		if filter.Node != "" && rec.Node != filter.Node {
			continue
		}
		if filter.Status != "" && rec.Status != filter.Status {
			continue
		}
		if filter.Start != nil && rec.LastUpdatedAt.Before(*filter.Start) {
			continue
		}
		if filter.End != nil && rec.LastUpdatedAt.After(*filter.End) {
			continue
		}
		result = append(result, *rec)
	}
	s.mu.RUnlock()

	sort.Slice(result, func(i, j int) bool {
		return result[i].LastUpdatedAt.After(result[j].LastUpdatedAt)
	})
	if filter.Limit > 0 && filter.Limit < len(result) {
		result = result[:filter.Limit]
	}
	return result
}

func (s *Store) persistLoop() {
	if s.filePath == "" {
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.save()
		}
	}
}

// stateSnapshot is the on-disk layout of the single state file.
type stateSnapshot struct {
	Version      int                 `json:"version"`
	Applications []ApplicationRecord `json:"applications"`
	Pods         []PodRecord         `json:"pods"`
}

func (s *Store) load() {
	if s.filePath == "" {
		return
	}
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return
	}
	var snapshot stateSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		klog.Warningf("Failed to parse state file %s: %v", s.filePath, err)
		return
	}

	s.mu.Lock()
	s.ingestLocked(snapshot.Applications, snapshot.Pods)
	if s.backfillClusterLocked() {
		s.dirty = true
	}
	s.mu.Unlock()
	recordStoreSizes(len(s.apps), len(s.pods))
	klog.Infof("Loaded %d applications and %d pod records from %s", len(snapshot.Applications), len(snapshot.Pods), s.filePath)
}

// backfillClusterLocked stamps the cluster identity onto persisted records
// written before it was known. Caller must hold the write lock.
func (s *Store) backfillClusterLocked() bool {
	if s.cluster == "" {
		return false
	}
	changed := false
	for _, rec := range s.apps {
		if rec.Cluster == "" {
			rec.Cluster = s.cluster
			changed = true
		}
	}
	for _, rec := range s.pods {
		if rec.Cluster == "" {
			rec.Cluster = s.cluster
			changed = true
		}
	}
	return changed
}

// ingestLocked indexes loaded records and rebuilds the change-time clock.
func (s *Store) ingestLocked(apps []ApplicationRecord, pods []PodRecord) {
	s.ingestAppsLocked(apps)
	for i := range pods {
		rec := pods[i]
		uid := rec.UID
		if uid == "" {
			uid = rec.Namespace + "/" + rec.Name
		}
		s.pods[uid] = &rec
		if rec.Role == "driver" {
			s.driverByApp[rec.ApplicationID] = uid
		}
	}
}

// ingestAppsLocked indexes application records and rebuilds the change-time
// clock. Records without a changedAt (older file layouts) are backfilled in
// lastUpdatedAt order, stepping by 1ms from the earliest timestamp.
func (s *Store) ingestAppsLocked(records []ApplicationRecord) {
	var missing []*ApplicationRecord
	var latest time.Time
	for i := range records {
		rec := records[i]
		s.apps[rec.ApplicationID] = &rec
		if rec.ChangedAt.After(latest) {
			latest = rec.ChangedAt
		}
		if rec.ChangedAt.IsZero() {
			missing = append(missing, s.apps[rec.ApplicationID])
		}
	}
	if len(missing) == 0 {
		if latest.After(s.lastChanged) {
			s.lastChanged = latest
		}
		return
	}

	sort.Slice(missing, func(i, j int) bool {
		return missing[i].LastUpdatedAt.Before(missing[j].LastUpdatedAt)
	})
	clock := missing[0].LastUpdatedAt.Add(-time.Millisecond)
	if clock.Before(latest) {
		clock = latest
	}
	for _, rec := range missing {
		clock = clock.Add(time.Millisecond)
		rec.ChangedAt = clock
	}
	if clock.After(s.lastChanged) {
		s.lastChanged = clock
	}
}

func (s *Store) save() {
	if s.filePath == "" {
		return
	}

	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return
	}
	defer measureFunc("Store.save")()
	s.dirty = false
	snapshot := stateSnapshot{
		Version:      1,
		Applications: make([]ApplicationRecord, 0, len(s.apps)),
		Pods:         make([]PodRecord, 0, len(s.pods)),
	}
	for _, rec := range s.apps {
		snapshot.Applications = append(snapshot.Applications, *rec)
	}
	for _, rec := range s.pods {
		snapshot.Pods = append(snapshot.Pods, *rec)
	}
	recordStoreSizes(len(snapshot.Applications), len(snapshot.Pods))
	s.mu.Unlock()

	if dir := filepath.Dir(s.filePath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			klog.Warningf("Failed to create state dir %s: %v", dir, err)
			recordSaveFailure()
			return
		}
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		recordSaveFailure()
		return
	}

	tmp := s.filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		klog.Warningf("Failed to write state file: %v", err)
		recordSaveFailure()
		return
	}
	if err := os.Rename(tmp, s.filePath); err != nil {
		klog.Warningf("Failed to finalize state file: %v", err)
		recordSaveFailure()
		return
	}
	recordSaveBytes(len(data))
}

// SparkRole returns the spark-role label (driver/executor) when the pod
// belongs to a Spark application with a resolvable applicationId.
func SparkRole(pod *corev1.Pod) string {
	if pod.Labels == nil {
		return ""
	}
	role := pod.Labels["spark-role"]
	if role != "driver" && role != "executor" {
		return ""
	}
	if AppIDFromPod(pod) == "" {
		return ""
	}
	return role
}

// isDriverPod reports whether the pod is a Spark driver.
func isDriverPod(pod *corev1.Pod) bool {
	return pod.Labels != nil && pod.Labels["spark-role"] == "driver"
}

// AppIDFromPod resolves the Spark application id, preferring the internal
// appSparkID label, then the applicationId label, and finally
// spark-app-selector for stock Spark deployments.
func AppIDFromPod(pod *corev1.Pod) string {
	if pod.Labels == nil {
		return ""
	}
	if appID := pod.Labels["appSparkID"]; appID != "" {
		return appID
	}
	// if appID := pod.Labels["applicationId"]; appID != "" {
	// 	return appID
	// }
	// return pod.Labels["spark-app-selector"]
	return ""
}

// podStatus derives a human-readable status from the pod phase and deleted flag.
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
