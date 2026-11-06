package store

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
)

func TestFileResumeStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pods-resume.json")
	s := NewFileResumeStore(path)
	ctx := context.Background()

	if rv, found, err := s.Load(ctx); err != nil || found || rv != "" {
		t.Fatalf("missing file should be not-found, got rv=%q found=%v err=%v", rv, found, err)
	}

	if err := s.Save(ctx, "12345"); err != nil {
		t.Fatalf("save: %v", err)
	}
	if rv, found, err := s.Load(ctx); err != nil || !found || rv != "12345" {
		t.Fatalf("expected resumed RV 12345, got rv=%q found=%v err=%v", rv, found, err)
	}
}

func TestNopResumeStore(t *testing.T) {
	s := NewNopResumeStore()
	if rv, found, err := s.Load(context.Background()); rv != "" || found || err != nil {
		t.Fatalf("nop load should be empty, got %q %v %v", rv, found, err)
	}
	if err := s.Save(context.Background(), "1"); err != nil {
		t.Fatalf("nop save: %v", err)
	}
}

func TestHTTPResumeStoreGetAndPut(t *testing.T) {
	var mu sync.Mutex
	stored := ""
	var lastMethod, lastContentType string
	var requestCount int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		requestCount++
		lastMethod = r.Method
		lastContentType = r.Header.Get("Content-Type")
		if r.URL.Path != "/checkpoints/podwatcher:resume:_all" {
			t.Errorf("unexpected checkpoint path: %s", r.URL.Path)
		}

		switch r.Method {
		case http.MethodGet:
			if stored == "" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(resumeState{ResourceVersion: stored})
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			var state resumeState
			if err := json.Unmarshal(body, &state); err != nil {
				t.Errorf("invalid envelope: %v", err)
			}
			stored = state.ResourceVersion
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	defer srv.Close()

	s := NewHTTPResumeStore(srv.URL, "podwatcher:resume:_all")
	ctx := context.Background()

	if rv, found, err := s.Load(ctx); err != nil || found || rv != "" {
		t.Fatalf("404 should be not-found, got rv=%q found=%v err=%v", rv, found, err)
	}
	if err := s.Save(ctx, "42"); err != nil {
		t.Fatalf("put after 404 baseline: %v", err)
	}
	rv, found, err := s.Load(ctx)
	if err != nil || !found || rv != "42" {
		t.Fatalf("expected RV 42 after put, got rv=%q found=%v err=%v", rv, found, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if lastMethod != http.MethodGet || lastContentType != "" {
		t.Fatalf("unexpected last request method=%s contentType=%q count=%d", lastMethod, lastContentType, requestCount)
	}
}

func TestHTTPResumeStorePutBlockedBeforeBaseline(t *testing.T) {
	var puts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		puts++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	s := NewHTTPResumeStore(srv.URL, "podwatcher:resume:_all")
	ctx := context.Background()

	if _, _, err := s.Load(ctx); err == nil {
		t.Fatal("expected error on 5xx baseline load")
	}
	if err := s.Save(ctx, "1"); err != nil {
		t.Fatalf("blocked save should be a silent no-op, got %v", err)
	}
	if puts != 0 {
		t.Fatalf("save must be blocked before a baseline, got %d puts", puts)
	}
}

func TestHTTPResumeStorePutError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := NewHTTPResumeStore(srv.URL, "podwatcher:resume:_all")
	ctx := context.Background()
	if _, _, err := s.Load(ctx); err != nil {
		t.Fatalf("404 baseline: %v", err)
	}
	if err := s.Save(ctx, "1"); err == nil {
		t.Fatal("expected error on 5xx checkpoint PUT")
	}
}
