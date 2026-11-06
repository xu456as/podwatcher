package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// ResumeStore persists the pod watch resourceVersion so a restart can resume
// from the last processed position instead of falling back to a full relist.
type ResumeStore interface {
	Load(ctx context.Context) (resourceVersion string, found bool, err error)
	Save(ctx context.Context, resourceVersion string) error
}

// resumeState is the JSON envelope persisted via a ResumeStore.
type resumeState struct {
	ResourceVersion string `json:"resourceVersion"`
}

// NopResumeStore disables resume persistence.
type NopResumeStore struct{}

func NewNopResumeStore() *NopResumeStore { return &NopResumeStore{} }

func (s *NopResumeStore) Load(context.Context) (string, bool, error) {
	return "", false, nil
}

func (s *NopResumeStore) Save(context.Context, string) error { return nil }

// FileResumeStore persists the envelope to a local JSON file (local dev).
type FileResumeStore struct {
	path string
}

func NewFileResumeStore(path string) *FileResumeStore { return &FileResumeStore{path: path} }

func (s *FileResumeStore) Load(context.Context) (string, bool, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, err
	}
	var state resumeState
	if err := json.Unmarshal(data, &state); err != nil {
		return "", false, err
	}
	return state.ResourceVersion, true, nil
}

func (s *FileResumeStore) Save(_ context.Context, rv string) error {
	if dir := filepath.Dir(s.path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	data, err := json.Marshal(resumeState{ResourceVersion: rv})
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// HTTPResumeStore persists the envelope through a downstream KV-style HTTP
// checkpoint API:
//
//	GET  {baseURL}/checkpoints/{key} -> 200 envelope | 404 not found
//	PUT  {baseURL}/checkpoints/{key}  body envelope
//
// Save is refused until the first successful Load (200 or 404) establishes a
// baseline, so a cold start against an unreachable API cannot overwrite a
// good checkpoint with full-relist state (split-brain guard).
type HTTPResumeStore struct {
	baseURL  string
	key      string
	client   *http.Client
	mu       sync.RWMutex
	baseline bool
	warnOnce sync.Once
}

func NewHTTPResumeStore(baseURL, key string) *HTTPResumeStore {
	return &HTTPResumeStore{
		baseURL: strings.TrimRight(baseURL, "/"),
		key:     key,
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (s *HTTPResumeStore) endpoint() string {
	return s.baseURL + "/checkpoints/" + url.PathEscape(s.key)
}

func (s *HTTPResumeStore) markBaseline() {
	s.mu.Lock()
	s.baseline = true
	s.mu.Unlock()
}

func (s *HTTPResumeStore) baselineReady() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.baseline
}

func (s *HTTPResumeStore) Load(ctx context.Context) (string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.endpoint(), nil)
	if err != nil {
		return "", false, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		s.markBaseline()
		return "", false, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", false, fmt.Errorf("checkpoint GET %s: %s: %s", s.key, resp.Status, strings.TrimSpace(string(body)))
	}

	var state resumeState
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return "", false, fmt.Errorf("decode checkpoint %s: %w", s.key, err)
	}
	s.markBaseline()
	return state.ResourceVersion, true, nil
}

func (s *HTTPResumeStore) Save(ctx context.Context, rv string) error {
	if !s.baselineReady() {
		s.warnOnce.Do(func() {
			klog.Warningf("Refusing to write checkpoint %s before a successful baseline load", s.key)
		})
		return nil
	}
	body, err := json.Marshal(resumeState{ResourceVersion: rv})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.endpoint(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("checkpoint PUT %s: %s: %s", s.key, resp.Status, strings.TrimSpace(string(data)))
	}
	return nil
}
