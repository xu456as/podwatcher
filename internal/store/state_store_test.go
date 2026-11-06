package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"podwatcher/internal/testutil"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestStoreDriverLifecycle(t *testing.T) {
	s := NewStore("", 0, 0)
	created := time.Now().Add(-time.Hour).Truncate(time.Second)
	pod := testutil.DriverPod("app-a-driver", "app-a", corev1.PodRunning, created)

	s.UpsertPod(pod, false)
	rec, ok := s.GetApplication("app-a")
	if !ok {
		t.Fatal("expected application record")
	}
	if rec.DriverPodName != "app-a-driver" || rec.Status != "running" || rec.DeletedAt != nil {
		t.Fatalf("unexpected running record: %+v", rec)
	}
	if !rec.CreatedAt.Equal(created) {
		t.Fatalf("createdAt should come from pod creationTimestamp, got %v want %v", rec.CreatedAt, created)
	}
	if rec.SparkAppName != "spark-pi-yunikorn" {
		t.Fatalf("expected spark app name preserved, got %q", rec.SparkAppName)
	}
	if rec.ChangedAt.IsZero() {
		t.Fatal("lifecycle creation must stamp changedAt")
	}

	s.UpsertPod(testutil.DriverPod("app-a-driver", "app-a", corev1.PodSucceeded, created), false)
	rec, _ = s.GetApplication("app-a")
	if rec.Status != "succeeded" || rec.FinishedAt == nil {
		t.Fatalf("expected succeeded with finishedAt: %+v", rec)
	}

	s.UpsertPod(testutil.DriverPod("app-a-driver", "app-a", corev1.PodSucceeded, created), true)
	rec, _ = s.GetApplication("app-a")
	if rec.Status != "deleted" || rec.DeletedAt == nil {
		t.Fatalf("expected deleted with deletedAt: %+v", rec)
	}
}

func TestStoreExecutorDoesNotDriveStatus(t *testing.T) {
	s := NewStore("", 0, 0)

	s.UpsertPod(testutil.ExecutorPod("app-e-exec-1", "app-e"), false)
	rec, ok := s.GetApplication("app-e")
	if !ok {
		t.Fatal("executor discovery should create an application record")
	}
	if rec.Status != "unknown" || rec.DriverPodName != "" || rec.DriverPhase != "" {
		t.Fatalf("executor-only record must stay unknown without driver, got %+v", rec)
	}

	s.UpsertPod(testutil.DriverPod("app-e-driver", "app-e", corev1.PodRunning, time.Now()), false)
	rec, _ = s.GetApplication("app-e")
	if rec.Status != "running" || rec.DriverPodName != "app-e-driver" {
		t.Fatalf("driver should drive the application status, got %+v", rec)
	}

	s.UpsertPod(testutil.ExecutorPod("app-e-exec-1", "app-e"), true)
	rec, _ = s.GetApplication("app-e")
	if rec.Status != "running" || rec.DeletedAt != nil {
		t.Fatalf("executor deletion must not affect application status, got %+v", rec)
	}
}

func TestStoreRequiresAppSparkID(t *testing.T) {
	s := NewStore("", 0, 0)
	pod := testutil.PlainPod("sel-driver", map[string]string{"spark-role": "driver", "spark-app-selector": "spark-selector-xyz"}, corev1.PodRunning)
	s.UpsertPod(pod, false)
	recs, _ := s.GetChanges(time.Time{}, 0, "", nil)
	if len(recs) != 0 {
		t.Fatalf("pods without appSparkID must not be tracked, got %+v", recs)
	}
}

func TestAppIDFromPod(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{name: "appSparkID takes precedence", labels: map[string]string{"appSparkID": "internal-1", "applicationId": "legacy-1", "spark-app-selector": "selector-1"}, want: "internal-1"},
		{name: "appSparkID only", labels: map[string]string{"appSparkID": "internal-2"}, want: "internal-2"},
		{name: "applicationId alone is ignored", labels: map[string]string{"applicationId": "legacy-3", "spark-app-selector": "selector-3"}, want: ""},
		{name: "selector alone is ignored", labels: map[string]string{"spark-app-selector": "selector-4"}, want: ""},
		{name: "empty values fall through", labels: map[string]string{"appSparkID": "", "applicationId": "", "spark-app-selector": ""}, want: ""},
		{name: "nil labels", labels: nil, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := testutil.PlainPod("probe-pod", tt.labels, corev1.PodRunning)
			if got := AppIDFromPod(pod); got != tt.want {
				t.Fatalf("AppIDFromPod() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStorePersistenceRoundTrip(t *testing.T) {
	file := filepath.Join(t.TempDir(), "state.json")

	s1 := NewStore(file, 10, 10)
	s1.UpsertPod(testutil.WithUID(testutil.DriverPod("app-d-driver", "app-d", corev1.PodSucceeded, time.Now().Add(-30*time.Minute)), "uid-d"), true)
	s1.Stop()

	s2 := NewStore(file, 10, 10)
	defer s2.Stop()
	recs, _ := s2.GetChanges(time.Time{}, 0, "", nil)
	if len(recs) != 1 {
		t.Fatalf("expected 1 loaded record, got %d", len(recs))
	}
	if recs[0].ApplicationID != "app-d" || recs[0].Status != "deleted" || recs[0].DeletedAt == nil {
		t.Fatalf("persisted deleted record mismatch: %+v", recs[0])
	}
}

func TestStorePersistenceContinuesChangedAt(t *testing.T) {
	file := filepath.Join(t.TempDir(), "state.json")

	s1 := NewStore(file, 10, 10)
	created := time.Now().Add(-time.Minute).Truncate(time.Second)
	s1.UpsertPod(testutil.WithUID(testutil.DriverPod("app-c-driver", "app-c", corev1.PodRunning, created), "uid-c"), false)
	s1.UpsertPod(testutil.WithUID(testutil.DriverPod("app-c-driver", "app-c", corev1.PodSucceeded, created), "uid-c"), false)
	s1.Stop()

	s2 := NewStore(file, 10, 10)
	defer s2.Stop()
	rec, ok := s2.GetApplication("app-c")
	if !ok || rec.ChangedAt.IsZero() {
		t.Fatalf("expected loaded record with changedAt, got %+v ok=%v", rec, ok)
	}
	loaded := rec.ChangedAt

	s2.UpsertPod(testutil.WithUID(testutil.DriverPod("app-c-driver", "app-c", corev1.PodSucceeded, created), "uid-c"), true)
	rec, _ = s2.GetApplication("app-c")
	if !rec.ChangedAt.After(loaded) {
		t.Fatalf("changedAt must continue after restored value, got %v want > %v", rec.ChangedAt, loaded)
	}
}

func TestStoreAppEvictionKeepsLive(t *testing.T) {
	s := NewStore("", 100, 0)
	base := time.Now().Truncate(time.Second)

	s.UpsertPod(testutil.DriverPod("old1-driver", "old1", corev1.PodSucceeded, base.Add(-3*time.Hour)), true)
	s.UpsertPod(testutil.DriverPod("old2-driver", "old2", corev1.PodSucceeded, base.Add(-2*time.Hour)), true)
	s.UpsertPod(testutil.DriverPod("old3-driver", "old3", corev1.PodSucceeded, base.Add(-time.Hour)), true)
	s.UpsertPod(testutil.DriverPod("live-driver", "live", corev1.PodRunning, base), false)

	s.mu.Lock()
	// Assign distinct deletion times so "oldest" is deterministic regardless of
	// wall-clock resolution across the rapid upserts.
	for id, offset := range map[string]time.Duration{
		"old1": -3 * time.Hour,
		"old2": -2 * time.Hour,
		"old3": -time.Hour,
	} {
		deletedAt := base.Add(offset)
		s.apps[id].DeletedAt = &deletedAt
	}
	s.maxApps = 3
	s.evictAppsLocked()
	s.mu.Unlock()

	recs, _ := s.GetChanges(time.Time{}, 0, "", nil)
	if len(recs) != 3 {
		t.Fatalf("expected 3 records after eviction (limit 3), got %d", len(recs))
	}
	if _, ok := s.GetApplication("old1"); ok {
		t.Fatal("oldest application should have been evicted")
	}
	if _, ok := s.GetApplication("live"); !ok {
		t.Fatal("live application must never be evicted")
	}
}

func TestStoreGetChangesByTimestamp(t *testing.T) {
	s := NewStore("", 0, 0)
	s.UpsertPod(testutil.WithUID(testutil.DriverPod("a-driver", "app-a", corev1.PodRunning, time.Now()), "a"), false)
	s.UpsertPod(testutil.WithUID(testutil.DriverPod("b-driver", "app-b", corev1.PodSucceeded, time.Now()), "b"), true)

	all, hasMore := s.GetChanges(time.Time{}, 0, "", nil)
	if len(all) != 2 || hasMore {
		t.Fatalf("expected 2 changes without paging, got %d hasMore=%v", len(all), hasMore)
	}
	if all[0].ApplicationID != "app-a" || all[1].ApplicationID != "app-b" {
		t.Fatalf("expected changedAt ascending order, got %+v", all)
	}

	caughtUp, _ := s.GetChanges(all[len(all)-1].ChangedAt, 0, "", nil)
	if len(caughtUp) != 0 {
		t.Fatalf("expected empty page at high water, got %+v", caughtUp)
	}

	page, pageMore := s.GetChanges(time.Time{}, 1, "", nil)
	if len(page) != 1 || !pageMore || page[0].ApplicationID != "app-a" {
		t.Fatalf("expected first page with app-a and hasMore, got %+v more=%v", page, pageMore)
	}
	page2, page2More := s.GetChanges(page[0].ChangedAt, 1, "", nil)
	if len(page2) != 1 || page2More || page2[0].ApplicationID != "app-b" {
		t.Fatalf("expected second page with app-b, got %+v more=%v", page2, page2More)
	}

	if running, _ := s.GetChanges(time.Time{}, 0, "", []string{"running"}); len(running) != 1 || running[0].ApplicationID != "app-a" {
		t.Fatalf("status filter failed: %+v", running)
	}
	if both, _ := s.GetChanges(time.Time{}, 0, "", []string{"running", "deleted"}); len(both) != 2 {
		t.Fatalf("multi-status filter should return both records, got %+v", both)
	}
	if one, _ := s.GetChanges(time.Time{}, 0, "app-a", nil); len(one) != 1 || one[0].ApplicationID != "app-a" {
		t.Fatalf("applicationId filter should return only app-a, got %+v", one)
	}
	if none, _ := s.GetChanges(time.Time{}, 0, "app-missing", nil); len(none) != 0 {
		t.Fatalf("unknown applicationId should match nothing, got %+v", none)
	}
}

func TestStoreChangedAtLifecycleOnly(t *testing.T) {
	s := NewStore("", 0, 0)

	s.UpsertPod(testutil.ExecutorPod("app-r-exec-1", "app-r"), false)
	rec, ok := s.GetApplication("app-r")
	if !ok || rec.Status != "unknown" || rec.ChangedAt.IsZero() {
		t.Fatalf("new executor record should be unknown with changedAt, got %+v ok=%v", rec, ok)
	}
	first := rec.ChangedAt
	s.UpsertPod(testutil.ExecutorPod("app-r-exec-2", "app-r"), false)
	s.UpsertPod(testutil.ExecutorPod("app-r-exec-1", "app-r"), true)
	rec, _ = s.GetApplication("app-r")
	if !rec.ChangedAt.Equal(first) {
		t.Fatalf("executor churn must not advance changedAt, got %v want %v", rec.ChangedAt, first)
	}

	created := time.Now().Add(-time.Minute).Truncate(time.Second)
	cases := []struct {
		name    string
		pod     *corev1.Pod
		deleted bool
		status  string
	}{
		{"running driver", testutil.DriverPod("app-r-driver", "app-r", corev1.PodRunning, created), false, "running"},
		{"succeeded driver", testutil.DriverPod("app-r-driver", "app-r", corev1.PodSucceeded, created), false, "succeeded"},
		{"deleted driver", testutil.DriverPod("app-r-driver", "app-r", corev1.PodSucceeded, created), true, "deleted"},
	}
	prev := first
	for _, tc := range cases {
		s.UpsertPod(tc.pod, tc.deleted)
		rec, _ = s.GetApplication("app-r")
		if !rec.ChangedAt.After(prev) || rec.Status != tc.status {
			t.Fatalf("%s: expected changedAt after %v and status %s, got %+v", tc.name, prev, tc.status, rec)
		}
		prev = rec.ChangedAt
	}
	if rec.FinishedAt == nil || rec.DeletedAt == nil {
		t.Fatalf("expected finishedAt and deletedAt set, got %+v", rec)
	}

	s.UpsertPod(testutil.DriverPod("app-r-driver", "app-r", corev1.PodSucceeded, created), true)
	rec, _ = s.GetApplication("app-r")
	if !rec.ChangedAt.Equal(prev) {
		t.Fatalf("duplicate terminal upsert must not advance changedAt, got %v want %v", rec.ChangedAt, prev)
	}
}

func TestStoreChangedAtStrictlyIncreasing(t *testing.T) {
	s := NewStore("", 0, 0)

	const n = 50
	for i := 0; i < n; i++ {
		appID := fmt.Sprintf("app-bulk-%d", i)
		s.UpsertPod(testutil.ExecutorPod(appID+"-exec", appID), false)
	}

	recs, _ := s.GetChanges(time.Time{}, 0, "", nil)
	if len(recs) != n {
		t.Fatalf("expected %d records, got %d", n, len(recs))
	}
	for i := 1; i < len(recs); i++ {
		gap := recs[i].ChangedAt.Sub(recs[i-1].ChangedAt)
		if gap < time.Millisecond {
			t.Fatalf("changedAt gap below 1ms at %d: %v (%v -> %v)", i, gap, recs[i-1].ChangedAt, recs[i].ChangedAt)
		}
	}
}

func TestStoreBootstrapAnchorsChangedAtReal(t *testing.T) {
	s := NewStore("", 0, 0)
	created := time.Now().Add(-time.Hour).Truncate(time.Second)

	s.BootstrapPod(testutil.DriverPod("app-b-driver", "app-b", corev1.PodRunning, created))
	rec, ok := s.GetApplication("app-b")
	if !ok {
		t.Fatal("expected bootstrapped application record")
	}
	if rec.Status != "running" || rec.DriverPodName != "app-b-driver" {
		t.Fatalf("bootstrap should populate state fields, got %+v", rec)
	}
	if !rec.ChangedAt.Equal(created) {
		t.Fatalf("bootstrap changedAt should anchor to creationTimestamp, got %v want %v", rec.ChangedAt, created)
	}

	// Anchored in the past: a since cursor at/after creation sees no change.
	if changes, _ := s.GetChanges(created, 0, "", nil); len(changes) != 0 {
		t.Fatalf("bootstrap must not appear after a caught-up cursor, got %+v", changes)
	}
}

func TestStoreBootstrapTerminalAnchorsFinishedAt(t *testing.T) {
	s := NewStore("", 0, 0)
	created := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	finished := time.Now().Add(-30 * time.Minute).Truncate(time.Second)
	pod := testutil.DriverPod("app-f-driver", "app-f", corev1.PodSucceeded, created)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "spark-kubernetes-driver",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			StartedAt:  metav1.NewTime(created),
			FinishedAt: metav1.NewTime(finished),
		}},
	}}

	s.BootstrapPod(pod)
	rec, _ := s.GetApplication("app-f")
	if rec.FinishedAt == nil || !rec.ChangedAt.Equal(finished) {
		t.Fatalf("terminal bootstrap should anchor changedAt to finishedAt, got %+v", rec)
	}
}

func TestStoreBootstrapZeroTimeFallsBackToNow(t *testing.T) {
	s := NewStore("", 0, 0)
	pod := testutil.DriverPod("app-z-driver", "app-z", corev1.PodRunning, time.Time{})
	pod.ObjectMeta.CreationTimestamp = metav1.Time{}
	before := time.Now()

	s.BootstrapPod(pod)
	rec, _ := s.GetApplication("app-z")
	if rec.ChangedAt.IsZero() || rec.ChangedAt.Before(before) {
		t.Fatalf("bootstrap without history should fall back to now, got %v", rec.ChangedAt)
	}
}

func TestStoreBootstrapDoesNotMoveExisting(t *testing.T) {
	s := NewStore("", 0, 0)
	created := time.Now().Add(-time.Hour).Truncate(time.Second)
	s.UpsertPod(testutil.DriverPod("app-e-driver", "app-e", corev1.PodRunning, created), false)
	rec, _ := s.GetApplication("app-e")
	changed := rec.ChangedAt

	s.BootstrapPod(testutil.DriverPod("app-e-driver", "app-e", corev1.PodRunning, created))
	rec, _ = s.GetApplication("app-e")
	if !rec.ChangedAt.Equal(changed) {
		t.Fatalf("bootstrap of an existing record must not move changedAt, got %v want %v", rec.ChangedAt, changed)
	}
}

func TestStoreRealEventAfterBootstrapAdvances(t *testing.T) {
	s := NewStore("", 0, 0)
	created := time.Now().Add(-time.Hour).Truncate(time.Second)
	s.BootstrapPod(testutil.DriverPod("app-a-driver", "app-a", corev1.PodRunning, created))

	s.UpsertPod(testutil.DriverPod("app-a-driver", "app-a", corev1.PodSucceeded, created), false)
	rec, _ := s.GetApplication("app-a")
	if rec.Status != "succeeded" || !rec.ChangedAt.After(created) {
		t.Fatalf("real terminal event should advance anchored changedAt, got %+v", rec)
	}
	changes, _ := s.GetChanges(created, 0, "", nil)
	if len(changes) != 1 || changes[0].ApplicationID != "app-a" {
		t.Fatalf("real event should be visible after the bootstrap anchor, got %+v", changes)
	}
}

func TestStoreOneApplicationManyPods(t *testing.T) {
	s := NewStore("", 0, 0)
	driver := testutil.WithUID(testutil.DriverPod("app-n-driver", "app-n", corev1.PodRunning, time.Now()), "uid-driver")
	exec1 := testutil.WithUID(testutil.ExecutorPod("app-n-exec-1", "app-n"), "uid-exec-1")
	exec2 := testutil.WithUID(testutil.ExecutorPod("app-n-exec-2", "app-n"), "uid-exec-2")

	s.UpsertPod(driver, false)
	s.UpsertPod(exec1, false)
	s.UpsertPod(exec2, false)

	all := s.GetPodRecords(PodRecordFilter{})
	if len(all) != 3 {
		t.Fatalf("expected 3 pod records, got %+v", all)
	}
	drivers := s.GetPodRecords(PodRecordFilter{Role: "driver"})
	if len(drivers) != 1 || drivers[0].Node != "node-1" || drivers[0].ExecutorID != "" {
		t.Fatalf("unexpected driver record: %+v", drivers)
	}
	execs := s.GetPodRecords(PodRecordFilter{ApplicationID: "app-n", Role: "executor"})
	if len(execs) != 2 {
		t.Fatalf("expected 2 executors, got %+v", execs)
	}
	for _, rec := range execs {
		if rec.ExecutorID == "" || rec.Node != "node-2" {
			t.Fatalf("executor identity fields not captured: %+v", rec)
		}
	}

	s.UpsertPod(testutil.WithUID(testutil.ExecutorPod("app-n-exec-1", "app-n"), "uid-exec-1"), true)
	deleted := s.GetPodRecords(PodRecordFilter{ApplicationID: "app-n", Status: "deleted"})
	if len(deleted) != 1 || deleted[0].Name != "app-n-exec-1" || deleted[0].DeletedAt == nil {
		t.Fatalf("expected 1 deleted executor, got %+v", deleted)
	}
}

func TestStoreContainerTimesAndDeletionTimestamp(t *testing.T) {
	s := NewStore("", 0, 0)
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	finish := time.Now().Add(-time.Minute).Truncate(time.Second)
	pod := testutil.WithUID(testutil.DriverPod("app-t-driver", "app-t", corev1.PodSucceeded, start), "uid-t")
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Name: "spark-kubernetes-driver",
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					StartedAt:  metav1.NewTime(start),
					FinishedAt: metav1.NewTime(finish),
				},
			},
		},
	}
	pod.ObjectMeta.DeletionTimestamp = &metav1.Time{Time: finish.Add(10 * time.Second)}

	s.UpsertPod(pod, false)
	recs := s.GetPodRecords(PodRecordFilter{ApplicationID: "app-t"})
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %+v", recs)
	}
	rec := recs[0]
	if rec.ContainerStartedAt == nil || !rec.ContainerStartedAt.Equal(start) {
		t.Fatalf("expected containerStartedAt %v, got %v", start, rec.ContainerStartedAt)
	}
	if rec.ContainerFinishedAt == nil || !rec.ContainerFinishedAt.Equal(finish) {
		t.Fatalf("expected containerFinishedAt %v, got %v", finish, rec.ContainerFinishedAt)
	}
	if rec.DeletionTimestamp == nil || !rec.DeletionTimestamp.Equal(finish.Add(10*time.Second)) {
		t.Fatalf("expected deletionTimestamp, got %v", rec.DeletionTimestamp)
	}
}

func TestStoreRunningContainerStartTime(t *testing.T) {
	s := NewStore("", 0, 0)
	start := time.Now().Add(-time.Minute).Truncate(time.Second)
	pod := testutil.WithUID(testutil.DriverPod("app-r-driver", "app-r", corev1.PodRunning, time.Now()), "uid-r")
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Name: "spark-kubernetes-driver",
			State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(start)},
			},
		},
	}

	s.UpsertPod(pod, false)
	recs := s.GetPodRecords(PodRecordFilter{ApplicationID: "app-r"})
	if len(recs) != 1 || recs[0].ContainerStartedAt == nil || !recs[0].ContainerStartedAt.Equal(start) {
		t.Fatalf("expected running container startedAt, got %+v", recs)
	}
	if recs[0].ContainerFinishedAt != nil {
		t.Fatalf("running container must not have finishedAt, got %v", recs[0].ContainerFinishedAt)
	}
}

func TestStorePodEviction(t *testing.T) {
	s := NewStore("", 0, 0)
	base := time.Now().Truncate(time.Second)

	s.UpsertPod(testutil.WithUID(testutil.DriverPod("old1-driver", "old1", corev1.PodSucceeded, base.Add(-3*time.Hour)), "old1"), true)
	s.UpsertPod(testutil.WithUID(testutil.ExecutorPod("old1-exec-1", "old1"), "old1-exec-1"), true)
	s.UpsertPod(testutil.WithUID(testutil.DriverPod("live-driver", "live", corev1.PodRunning, base), "live"), false)

	s.mu.Lock()
	// Three records exist (two retained drivers, one deleted executor). Anchor
	// the executor deletion time, then lower the pod limit: only the executor is
	// reclaimable, while both drivers (including the deleted retained driver)
	// stay protected.
	execDeletedAt := base.Add(-90 * time.Minute)
	s.pods["old1-exec-1"].DeletedAt = &execDeletedAt
	s.maxPods = 2
	removed := s.evictPodsLocked()
	s.mu.Unlock()

	if removed != 1 {
		t.Fatalf("expected 1 reclaimed executor, got %d", removed)
	}
	if recs := s.GetPodRecords(PodRecordFilter{}); len(recs) != 2 {
		t.Fatalf("expected 2 records after pod eviction, got %+v", recs)
	}
	if _, ok := s.GetApplication("old1"); !ok {
		t.Fatal("retained application must still exist")
	}
	if recs := s.GetPodRecords(PodRecordFilter{Role: "executor"}); len(recs) != 0 {
		t.Fatalf("retained app executor should have been removed, got %+v", recs)
	}

	// A deleted pod whose application no longer exists (orphan) is reclaimable
	// even when it is a driver.
	s.mu.Lock()
	orphanDeletedAt := base.Add(-4 * time.Hour)
	s.pods["orphan-driver"] = &PodRecord{
		UID: "orphan-driver", ApplicationID: "ghost", Role: "driver",
		Status: "deleted", DeletedAt: &orphanDeletedAt,
	}
	s.maxPods = 2
	orphanRemoved := s.evictPodsLocked()
	s.mu.Unlock()
	if orphanRemoved != 1 {
		t.Fatalf("expected orphan driver reclaimed, got %d", orphanRemoved)
	}
	if _, ok := s.pods["orphan-driver"]; ok {
		t.Fatal("orphan driver should have been removed")
	}
}

func TestStoreLimitAndOrder(t *testing.T) {
	s := NewStore("", 0, 0)
	s.UpsertPod(testutil.WithUID(testutil.ExecutorPod("app-l-exec-1", "app-l"), "e1"), false)
	time.Sleep(2 * time.Millisecond)
	s.UpsertPod(testutil.WithUID(testutil.ExecutorPod("app-l-exec-2", "app-l"), "e2"), false)

	recs := s.GetPodRecords(PodRecordFilter{Limit: 1})
	if len(recs) != 1 || recs[0].Name != "app-l-exec-2" {
		t.Fatalf("limit should return only the newest record, got %+v", recs)
	}
}

func TestStoreCapturesLabels(t *testing.T) {
	s := NewStore("", 0, 0)
	labels := map[string]string{
		"spark-role":         "executor",
		"appSparkID":         "app-l",
		"applicationId":      "app-l",
		"spark-app-selector": "app-l",
		"spark-exec-id":      "1",
		"custom-label":       "v",
	}
	pod := testutil.WithUID(testutil.PlainPod("app-l-exec-1", labels, corev1.PodRunning), "uid-labels")
	s.UpsertPod(pod, false)

	recs := s.GetPodRecords(PodRecordFilter{ApplicationID: "app-l"})
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %+v", recs)
	}
	got := recs[0].Labels
	if len(got) != len(labels) {
		t.Fatalf("expected %d labels, got %d: %+v", len(labels), len(got), got)
	}
	for key, want := range labels {
		if got[key] != want {
			t.Fatalf("label %s = %q, want %q", key, got[key], want)
		}
	}

	// The stored labels must be a defensive copy, not the informer's map.
	labels["custom-label"] = "mutated"
	if got["custom-label"] != "v" {
		t.Fatalf("record labels must be a defensive copy, got %q", got["custom-label"])
	}

	// Labels must survive deletion (the tombstone still carries labels).
	s.UpsertPod(testutil.WithUID(testutil.PlainPod("app-l-exec-1", labels, corev1.PodRunning), "uid-labels"), true)
	deleted := s.GetPodRecords(PodRecordFilter{ApplicationID: "app-l", Status: "deleted"})
	if len(deleted) != 1 || len(deleted[0].Labels) == 0 {
		t.Fatalf("labels must survive deletion, got %+v", deleted)
	}
}

func TestStoreBootstrapAnchorsPodLastUpdated(t *testing.T) {
	s := NewStore("", 0, 0)
	created := time.Now().Add(-time.Hour).Truncate(time.Second)
	started := time.Now().Add(-30 * time.Minute).Truncate(time.Second)
	pod := testutil.WithUID(testutil.DriverPod("app-b-driver", "app-b", corev1.PodRunning, created), "uid-b")
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "spark-kubernetes-driver",
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(started)}},
	}}

	s.BootstrapPod(pod)
	recs := s.GetPodRecords(PodRecordFilter{ApplicationID: "app-b"})
	if len(recs) != 1 {
		t.Fatalf("expected 1 bootstrapped record, got %+v", recs)
	}
	if !recs[0].LastUpdatedAt.Equal(started) {
		t.Fatalf("bootstrap should anchor lastUpdatedAt to container start, got %v want %v", recs[0].LastUpdatedAt, started)
	}
	if recs[0].DeletedAt != nil {
		t.Fatalf("bootstrap must never mark a record deleted, got %+v", recs[0])
	}

	// A time window ending before the anchor must not return the record.
	windowEnd := started.Add(-time.Minute)
	if old := s.GetPodRecords(PodRecordFilter{End: &windowEnd}); len(old) != 0 {
		t.Fatalf("bootstrapped record must not appear in a pre-start window, got %+v", old)
	}
}

func TestStoreBootstrapPodFallsBackToCreationTimestamp(t *testing.T) {
	s := NewStore("", 0, 0)
	created := time.Now().Add(-time.Hour).Truncate(time.Second)
	pod := testutil.WithUID(testutil.DriverPod("app-c-driver", "app-c", corev1.PodPending, created), "uid-c")

	s.BootstrapPod(pod)
	recs := s.GetPodRecords(PodRecordFilter{ApplicationID: "app-c"})
	if len(recs) != 1 || !recs[0].LastUpdatedAt.Equal(created) {
		t.Fatalf("bootstrap should anchor to creationTimestamp without container times, got %+v", recs)
	}
}

func TestStoreCascadeEvictionDeletesPods(t *testing.T) {
	s := NewStore("", 2, 0)
	base := time.Now().Truncate(time.Second)

	s.UpsertPod(testutil.WithUID(testutil.DriverPod("a-driver", "a", corev1.PodSucceeded, base.Add(-3*time.Hour)), "a"), true)
	s.UpsertPod(testutil.WithUID(testutil.ExecutorPod("a-exec-1", "a"), "a-exec-1"), true)
	s.UpsertPod(testutil.WithUID(testutil.DriverPod("b-driver", "b", corev1.PodSucceeded, base.Add(-2*time.Hour)), "b"), true)
	s.mu.Lock()
	deletedA := base.Add(-3 * time.Hour)
	deletedB := base.Add(-2 * time.Hour)
	s.apps["a"].DeletedAt = &deletedA
	s.apps["b"].DeletedAt = &deletedB
	s.mu.Unlock()
	s.UpsertPod(testutil.WithUID(testutil.DriverPod("live-driver", "live", corev1.PodRunning, base), "live"), false)

	// When the third application arrives the oldest one (a) is evicted and its
	// driver/executor records leave in the same operation.
	if _, ok := s.GetApplication("a"); ok {
		t.Fatal("evicted application should be gone")
	}
	if recs := s.GetPodRecords(PodRecordFilter{ApplicationID: "a"}); len(recs) != 0 {
		t.Fatalf("evicted application pods should be cascade-deleted, got %+v", recs)
	}
	s.mu.RLock()
	_, indexed := s.driverByApp["a"]
	s.mu.RUnlock()
	if indexed {
		t.Fatal("evicted application driver index should be removed")
	}
	if _, ok := s.GetApplication("live"); !ok {
		t.Fatal("live application must be retained")
	}
	if recs := s.GetPodRecords(PodRecordFilter{ApplicationID: "live"}); len(recs) != 1 {
		t.Fatalf("live application driver should remain, got %+v", recs)
	}
}

func TestStoreApplicationViewJoin(t *testing.T) {
	s := NewStore("", 0, 0)
	pod := testutil.WithUID(testutil.DriverPod("app-n-driver", "app-n", corev1.PodRunning, time.Now()), "uid-n")
	s.UpsertPod(pod, false)

	view, ok := s.GetApplication("app-n")
	if !ok {
		t.Fatal("expected joined application view")
	}
	if view.DriverPhase != "Running" {
		t.Fatalf("driverPhase should come from the driver pod, got %q", view.DriverPhase)
	}
	if view.DriverLabels["spark-role"] != "driver" || view.DriverLabels["appSparkID"] != "app-n" {
		t.Fatalf("driverLabels should be joined from the driver pod, got %+v", view.DriverLabels)
	}

	// Executor-first discovery has no driver fields until the driver appears.
	s2 := NewStore("", 0, 0)
	s2.UpsertPod(testutil.ExecutorPod("app-e-exec-1", "app-e"), false)
	view2, _ := s2.GetApplication("app-e")
	if view2.DriverPhase != "" || len(view2.DriverLabels) != 0 {
		t.Fatalf("executor-only view must have empty driver fields, got %+v", view2)
	}
	s2.UpsertPod(testutil.WithUID(testutil.DriverPod("app-e-driver", "app-e", corev1.PodSucceeded, time.Now()), "uid-e-d"), false)
	view2, _ = s2.GetApplication("app-e")
	if view2.DriverPhase != "Succeeded" {
		t.Fatalf("driver fields should populate once the driver is observed, got %+v", view2)
	}
}

func TestStoreImportLegacy(t *testing.T) {
	dir := t.TempDir()
	appFile := filepath.Join(dir, "applications.json")
	podFile := filepath.Join(dir, "pod-records.json")
	stateFile := filepath.Join(dir, "state.json")

	applications := []ApplicationRecord{
		{ApplicationID: "app-a", Status: "deleted", LastUpdatedAt: time.Now().Add(-time.Hour)},
	}
	wrapped := struct {
		Revision int64               `json:"revision"`
		Records  []ApplicationRecord `json:"records"`
	}{Revision: 1, Records: applications}
	data, err := json.Marshal(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(appFile, data, 0o644); err != nil {
		t.Fatal(err)
	}

	pods := []PodRecord{
		{UID: "uid-d", ApplicationID: "app-a", Namespace: "default", Name: "app-a-driver", Role: "driver", Phase: "Succeeded", Status: "deleted"},
	}
	data, err = json.Marshal(pods)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(podFile, data, 0o644); err != nil {
		t.Fatal(err)
	}

	s := NewStore(stateFile, 0, 0)
	s.ImportLegacy(appFile, podFile)
	if recs, _ := s.GetChanges(time.Time{}, 0, "", nil); len(recs) != 1 || recs[0].ApplicationID != "app-a" {
		t.Fatalf("expected legacy application imported, got %+v", recs)
	}
	if recs := s.GetPodRecords(PodRecordFilter{ApplicationID: "app-a"}); len(recs) != 1 {
		t.Fatalf("expected legacy pod record imported, got %+v", recs)
	}
	s.Stop()

	// After migration the state loads from the new single file, not the legacy
	// sources.
	s2 := NewStore(stateFile, 0, 0)
	defer s2.Stop()
	if recs, _ := s2.GetChanges(time.Time{}, 0, "", nil); len(recs) != 1 {
		t.Fatalf("expected migrated state to reload, got %+v", recs)
	}
}

func TestStoreStampsCluster(t *testing.T) {
	s := NewStore("", 0, 0, WithCluster("cluster-a"))

	s.UpsertPod(testutil.DriverPod("app-c-driver", "app-c", corev1.PodRunning, time.Now()), false)
	app, ok := s.GetApplication("app-c")
	if !ok {
		t.Fatal("expected application record")
	}
	if app.Cluster != "cluster-a" {
		t.Fatalf("application cluster = %q want cluster-a", app.Cluster)
	}

	s.UpsertPod(testutil.ExecutorPod("app-c-exec-1", "app-c"), false)
	for _, rec := range s.GetPodRecords(PodRecordFilter{ApplicationID: "app-c"}) {
		if rec.Cluster != "cluster-a" {
			t.Fatalf("pod record %s cluster = %q want cluster-a", rec.Name, rec.Cluster)
		}
	}
}

func TestStoreBackfillsClusterOnLoad(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	snapshot := stateSnapshot{
		Version:      1,
		Applications: []ApplicationRecord{{ApplicationID: "old", Status: "running", ChangedAt: time.Now()}},
		Pods: []PodRecord{{
			UID: "old-pod", ApplicationID: "old", Name: "old-driver", Role: "driver", Status: "running",
		}},
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stateFile, data, 0o644); err != nil {
		t.Fatal(err)
	}

	s := NewStore(stateFile, 10, 10, WithCluster("cluster-a"))
	defer s.Stop()

	app, ok := s.GetApplication("old")
	if !ok {
		t.Fatal("expected loaded application")
	}
	if app.Cluster != "cluster-a" {
		t.Fatalf("backfilled application cluster = %q want cluster-a", app.Cluster)
	}
	for _, rec := range s.GetPodRecords(PodRecordFilter{}) {
		if rec.Cluster != "cluster-a" {
			t.Fatalf("backfilled pod record cluster = %q want cluster-a", rec.Cluster)
		}
	}
}
