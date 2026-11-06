package informer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"podwatcher/internal/handler"
	"podwatcher/internal/store"
	"podwatcher/pkg/k8s"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func testPod(namespace, name, rv string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       namespace,
			Name:            name,
			UID:             types.UID(namespace + "/" + name),
			ResourceVersion: rv,
			Labels:          labels,
		},
	}
}

func newTestEventHandler() *handler.PodEventHandler {
	return handler.NewPodEventHandler(store.NewStore("", 0, 0))
}

func newTestPodManager(pods ...*corev1.Pod) *PodInformerManager {
	objs := make([]runtime.Object, 0, len(pods))
	for _, p := range pods {
		objs = append(objs, p)
	}
	client := &k8s.Client{Clientset: fake.NewSimpleClientset(objs...)}
	return NewPodInformerManager(client, newTestEventHandler(), store.NewNopResumeStore())
}

func TestPodRelistInitial(t *testing.T) {
	p1 := testPod("default", "spark-driver", "10", map[string]string{"appSparkID": "app-1", "spark-role": "driver"})
	p2 := testPod("default", "spark-exec-1", "10", map[string]string{"appSparkID": "app-1", "spark-role": "executor"})
	p3 := testPod("default", "web", "11", nil)
	m := newTestPodManager(p1, p2, p3)

	if err := m.relist(true); err != nil {
		t.Fatalf("relist failed: %v", err)
	}

	events := m.handler.GetEvents(0)
	if len(events) != 1 {
		t.Fatalf("expected 1 INITIAL driver event, got %d", len(events))
	}
	for _, e := range events {
		if e.Type != "INITIAL" {
			t.Fatalf("expected INITIAL event type, got %q", e.Type)
		}
	}
	// Both driver and executor reach the pod store, even though the event
	// stream stays driver-only.
	pods := m.handler.GetPodRecords(store.PodRecordFilter{ApplicationID: "app-1"})
	if len(pods) != 2 {
		t.Fatalf("expected driver+executor pod records, got %+v", pods)
	}
	if _, found := m.LookupPod("default", "spark-driver"); !found {
		t.Fatal("spark-driver should be present in the store after relist")
	}
}

func TestPodReplayLifecycle(t *testing.T) {
	m := newTestPodManager()
	h := m.handler

	// Pod created and deleted while the watcher was down: it is not in the
	// snapshot/store, but the Deleted event must still be dispatched because
	// the deleted object carries its labels (e.g. appSparkID).
	gone := testPod("default", "gone-driver", "20", map[string]string{"appSparkID": "app-gone", "spark-role": "driver"})
	m.onDeleted(gone)
	events := h.GetEvents(0)
	if len(events) != 1 || events[0].Type != "DELETED" || events[0].Name != "gone-driver" {
		t.Fatalf("expected DELETED event for pod missing from store, got %+v", events)
	}
	apps, _ := h.GetApplications(time.Time{}, 0, "", nil)
	if len(apps) != 1 || apps[0].ApplicationID != "app-gone" || apps[0].Status != "deleted" {
		t.Fatalf("expected deleted application, got %+v", apps)
	}

	// Snapshot holds a newer version of a live pod; an older replayed Added
	// must be skipped (no event, store version unchanged).
	live := testPod("default", "live-driver", "100", map[string]string{"appSparkID": "app-live", "spark-role": "driver"})
	live.Status.Phase = corev1.PodRunning
	if err := m.store.Add(live); err != nil {
		t.Fatalf("store add: %v", err)
	}
	stale := testPod("default", "live-driver", "90", map[string]string{"appSparkID": "app-live", "spark-role": "driver"})
	before := len(h.GetEvents(0))
	m.onAdded(stale)
	if got := len(h.GetEvents(0)); got != before {
		t.Fatalf("older replayed Added must be skipped, got %d events", got)
	}
	if obj, found, _ := m.store.Get(live); found {
		if got := obj.(*corev1.Pod).ResourceVersion; got != "100" {
			t.Fatalf("store must keep newer RV 100, got %q", got)
		}
	}

	// A newer replayed Modified (phase -> Succeeded) is dispatched.
	newer := testPod("default", "live-driver", "110", map[string]string{"appSparkID": "app-live", "spark-role": "driver"})
	newer.Status.Phase = corev1.PodSucceeded
	m.onModified(newer)
	events = h.GetEvents(0)
	if len(events) != before+1 || events[0].Type != "MODIFIED" {
		t.Fatalf("expected 1 new MODIFIED event, got %+v", events)
	}

	// A brand-new pod replayed as Added is dispatched.
	fresh := testPod("default", "fresh-driver", "120", map[string]string{"appSparkID": "app-fresh", "spark-role": "driver"})
	m.onAdded(fresh)
	events = h.GetEvents(0)
	if len(events) != before+2 || events[0].Type != "ADDED" || events[0].Name != "fresh-driver" {
		t.Fatalf("expected ADDED event for fresh pod, got %+v", events)
	}
}

func TestPodClearRV(t *testing.T) {
	m := newTestPodManager()

	m.setRV("271062")
	if got := m.currentRV(); got != "271062" {
		t.Fatalf("expected RV 271062, got %q", got)
	}

	// clearRV must actually empty the RV (setRV("") is a no-op) so the next
	// listAndWatch falls back to a full relist after a 410 Gone.
	m.clearRV()
	if got := m.currentRV(); got != "" {
		t.Fatalf("expected empty RV after clearRV, got %q", got)
	}
}

func TestPodResumeFileStoreRoundTrip(t *testing.T) {
	resumePath := t.TempDir() + "/pods-resume.json"
	client := &k8s.Client{Clientset: fake.NewSimpleClientset()}

	m1 := NewPodInformerManager(client, newTestEventHandler(), store.NewFileResumeStore(resumePath))
	if got := m1.loadRV(); got != "" {
		t.Fatalf("expected empty RV when checkpoint is missing, got %q", got)
	}
	m1.setRV("12345")
	m1.saveRV()

	m2 := NewPodInformerManager(client, newTestEventHandler(), store.NewFileResumeStore(resumePath))
	if got := m2.loadRV(); got != "12345" {
		t.Fatalf("expected resumed RV 12345, got %q", got)
	}

	// A nop store disables persistence.
	m3 := NewPodInformerManager(client, newTestEventHandler(), store.NewNopResumeStore())
	m3.setRV("999")
	m3.saveRV()
	if got := m3.loadRV(); got != "" {
		t.Fatalf("expected persistence disabled, got %q", got)
	}
}

type fakeResumeStore struct {
	mu      sync.Mutex
	rv      string
	found   bool
	loadErr error
	saveErr error
	saves   []string
}

func (f *fakeResumeStore) Load(context.Context) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rv, f.found, f.loadErr
}

func (f *fakeResumeStore) Save(_ context.Context, rv string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return f.saveErr
	}
	f.saves = append(f.saves, rv)
	return nil
}

func (f *fakeResumeStore) saveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.saves)
}

func TestPodSaveRVCoalescesAndRetries(t *testing.T) {
	client := &k8s.Client{Clientset: fake.NewSimpleClientset()}
	rs := &fakeResumeStore{}
	m := NewPodInformerManager(client, newTestEventHandler(), rs)

	m.saveRV()
	if rs.saveCount() != 0 {
		t.Fatalf("expected no saves before the RV is dirty, got %v", rs.saves)
	}

	m.setRV("100")
	m.setRV("101")
	m.saveRV()
	if rs.saveCount() != 1 || rs.saves[0] != "101" {
		t.Fatalf("expected one coalesced save of the latest RV 101, got %v", rs.saves)
	}

	// Dirty flag is cleared after a successful flush.
	m.saveRV()
	if rs.saveCount() != 1 {
		t.Fatalf("expected no duplicate save, got %v", rs.saves)
	}

	// A failed save keeps dirty so the next flush retries.
	rs.saveErr = errors.New("checkpoint unavailable")
	m.setRV("102")
	m.saveRV()
	rs.saveErr = nil
	m.saveRV()
	if rs.saveCount() != 2 || rs.saves[1] != "102" {
		t.Fatalf("expected retry save of RV 102, got %v", rs.saves)
	}
}
func TestRVAtLeast(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"100", "100", true},
		{"101", "100", true},
		{"99", "100", false},
		{"abc", "abc", true},  // unparsable: equal strings count as same
		{"abc", "xyz", false}, // unparsable and different: not >=
	}
	for _, tc := range cases {
		if got := rvAtLeast(tc.a, tc.b); got != tc.want {
			t.Errorf("rvAtLeast(%q,%q)=%v want %v", tc.a, tc.b, got, tc.want)
		}
	}
}
