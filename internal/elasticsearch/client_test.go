package elasticsearch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"podwatcher/internal/config"
	"podwatcher/internal/podmetrics"
	"podwatcher/internal/prometheus"
	"podwatcher/internal/store"
	"podwatcher/internal/testutil"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestFailedDocumentDebugPayload(t *testing.T) {
	var sent []byte
	srv := newTestESServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events"}, "cluster")
	if err != nil {
		t.Fatal(err)
	}
	err = c.Publish(context.Background(), "ADDED", testutil.ExecutorPod("executor", "app"), false)
	var failure *PublishError
	if !errors.As(err, &failure) {
		t.Fatalf("expected payload error, got %v", err)
	}
	if failure.DebugPayload() != string(sent) {
		t.Fatal("debug payload must equal the actual request JSON")
	}
	if strings.Contains(err.Error(), "\"podName\"") {
		t.Fatal("payload must not appear in normal error message")
	}
}

func TestTimeoutConfiguration(t *testing.T) {
	for _, tc := range []struct {
		seconds int
		want    time.Duration
	}{{0, 20 * time.Second}, {7, 7 * time.Second}} {
		c, err := NewClient(config.ElasticsearchConfig{Address: "http://localhost:9200", Index: "events", TimeoutSeconds: tc.seconds}, "cluster")
		if err != nil {
			t.Fatal(err)
		}
		if c.timeout != tc.want {
			t.Fatalf("timeout=%v, want=%v", c.timeout, tc.want)
		}
	}
	if _, err := NewClient(config.ElasticsearchConfig{Address: "http://localhost:9200", Index: "events", TimeoutSeconds: -1}, "cluster"); err == nil {
		t.Fatal("negative timeout must fail")
	}
}

func TestSelfSignedTLSVerificationSwitch(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	for _, skip := range []bool{false, true} {
		c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events", InsecureSkipVerify: skip}, "cluster")
		if err != nil {
			t.Fatal(err)
		}
		err = c.Publish(context.Background(), "ADDED", testutil.ExecutorPod("executor", "app"), false)
		if skip && err != nil {
			t.Fatalf("self-signed TLS with skip enabled: %v", err)
		}
		if !skip && err == nil {
			t.Fatal("default must reject untrusted certificate")
		}
		if !skip && !strings.Contains(err.Error(), "certificate") {
			t.Fatalf("TLS diagnostic lost: %v", err)
		}
	}
}

func TestTopLevelQueryFields(t *testing.T) {
	for _, queue := range []string{"root.default", ""} {
		t.Run("queue="+queue, func(t *testing.T) {
			calls := 0
			srv := newTestESServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var doc map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
					t.Error(err)
					return
				}
				for key, want := range map[string]string{"applicationId": "app-a", "podName": "driver", "queue": queue} {
					var got string
					if err := json.Unmarshal(doc[key], &got); err != nil || got != want {
						t.Errorf("%s: got %q, want %q (err=%v)", key, got, want, err)
					}
				}
				w.WriteHeader(http.StatusCreated)
			}))
			defer srv.Close()
			c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events"}, "cluster")
			if err != nil {
				t.Fatal(err)
			}
			pod := testutil.DriverPod("driver", "app-a", corev1.PodSucceeded, time.Now())
			// Use the same canonical app id as the store even if another label differs.
			pod.Labels["applicationId"] = "other-id"
			if queue != "" {
				pod.Labels["queue"] = queue
			}
			for _, kind := range []string{"ADDED", "DELETED"} {
				if err := c.Publish(context.Background(), kind, pod, false); err != nil {
					t.Fatal(err)
				}
			}
			finished := time.Now()
			app := store.ApplicationView{ApplicationRecord: store.ApplicationRecord{ApplicationID: "app-a", DriverPodName: "driver", FinishedAt: &finished}, DriverLabels: pod.Labels}
			if err := c.PublishApplication(context.Background(), app); err != nil {
				t.Fatal(err)
			}
			if calls != 3 {
				t.Fatalf("got %d documents", calls)
			}
		})
	}
}

func TestPodEventLifecycleTimes(t *testing.T) {
	created := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	deletionRequested := created.Add(time.Minute)
	var events []Event
	srv := newTestESServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if _, exists := body["initial"]; exists {
			t.Error("ambiguous initial field must not be exported")
		}
		if _, exists := body["isInitialSnapshot"]; !exists {
			t.Error("snapshot marker missing")
		}
		data, _ := json.Marshal(body)
		var event Event
		if err := json.Unmarshal(data, &event); err != nil {
			t.Error(err)
		}
		events = append(events, event)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events"}, "cluster")
	if err != nil {
		t.Fatal(err)
	}
	pod := testutil.DriverPod("driver", "app", corev1.PodRunning, created)
	pod.DeletionTimestamp = &metav1.Time{Time: deletionRequested}
	before := time.Now().UTC()
	if err := c.Publish(context.Background(), "ADDED", pod, true); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(context.Background(), "DELETED", pod, false); err != nil {
		t.Fatal(err)
	}
	after := time.Now().UTC()
	for _, event := range events {
		if event.AddedAt == nil || !event.AddedAt.Equal(created) {
			t.Errorf("creation time lost: %+v", event)
		}
	}
	if !events[0].Initial || events[0].DeletedAt != nil {
		t.Error("snapshot add must not carry deletion time")
	}
	deleted := events[1].DeletedAt
	if deleted == nil || deleted.Before(before) || deleted.After(after) || deleted.Equal(deletionRequested) {
		t.Errorf("deletion must use observation time: %v", deleted)
	}
	pod.CreationTimestamp = metav1.Time{}
	if err := c.Publish(context.Background(), "ADDED", pod, false); err != nil {
		t.Fatal(err)
	}
	if events[2].AddedAt != nil {
		t.Error("unknown creation time must be omitted")
	}
}

func TestPublishCompletedApplication(t *testing.T) {
	finished := time.Now().Add(-time.Hour).UTC()
	var paths []string
	srv := newTestESServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		var doc Completion
		if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
			t.Error(err)
		}
		if doc.Type != "APPLICATION_COMPLETED" || doc.Cluster != "cluster" || !doc.FinishedAt.Equal(finished) || doc.IndexedAt.Before(finished) || doc.Application.Status != "succeeded" || doc.Application.ApplicationID != "app" {
			t.Errorf("incorrect completion: %+v", doc)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events"}, "cluster")
	if err != nil {
		t.Fatal(err)
	}
	app := store.ApplicationView{ApplicationRecord: store.ApplicationRecord{ApplicationID: "app", Namespace: "default", Status: "succeeded", FinishedAt: &finished}, DriverPhase: "Succeeded"}
	if err := c.PublishApplication(context.Background(), app); err != nil {
		t.Fatal(err)
	}
	app.Status = "deleted"
	if err := c.PublishApplication(context.Background(), app); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || paths[0] != paths[1] {
		t.Fatalf("one summary per application: %v", paths)
	}
	app.FinishedAt = nil
	if err := c.PublishApplication(context.Background(), app); err == nil {
		t.Fatal("missing completion time must fail")
	}
}

func TestPublishAuthenticationPayloadAndRetry(t *testing.T) {
	t.Setenv("TEST_ES_PASSWORD", "test-secret")
	var paths []string
	srv := newTestESServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "elastic" || pass != "test-secret" {
			t.Error("incorrect auth")
		}
		for _, header := range []string{"Accept", "Content-Type"} {
			if got := r.Header.Get(header); !strings.Contains(got, "compatible-with=8") {
				t.Errorf("%s must request v8 compatibility, got %q", header, got)
			}
		}
		if r.Method != http.MethodPut || !strings.HasPrefix(r.URL.Path, "/events/_doc/") {
			t.Errorf("incorrect request %s %s", r.Method, r.URL.Path)
		}
		paths = append(paths, r.URL.Path)
		var event Event
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		if event.Cluster != "cluster-a" || event.Type != "ADDED" || event.Initial || event.Node != "node-1" || event.Timestamp.IsZero() {
			t.Errorf("incorrect event: %+v", event)
		}
		if len(paths) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Username: "elastic", PasswordEnv: "TEST_ES_PASSWORD", Index: "events"}, "cluster-a")
	if err != nil {
		t.Fatal(err)
	}
	pod := testutil.WithUID(testutil.DriverPod("driver", "app-a", corev1.PodRunning, time.Now()), "uid-1")
	pod.ResourceVersion = "123"
	if err := c.Publish(context.Background(), "ADDED", pod, false); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || paths[0] != paths[1] {
		t.Fatalf("retry must keep document id: %v", paths)
	}
}

func TestPublishRejectsPermanentFailureWithoutRetry(t *testing.T) {
	calls := 0
	srv := newTestESServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("sensitive response"))
	}))
	defer srv.Close()
	c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events"}, "cluster")
	if err != nil {
		t.Fatal(err)
	}
	err = c.Publish(context.Background(), "DELETED", testutil.ExecutorPod("executor", "app"), false)
	if err == nil || calls != 1 || strings.Contains(err.Error(), "sensitive") {
		t.Fatalf("unexpected error/calls: %v / %d", err, calls)
	}
}

func TestPublishErrorDetails(t *testing.T) {
	t.Setenv("TEST_ES_PASSWORD", "test-secret")
	for _, tc := range []struct {
		name string
		body string
		want []string
	}{
		{"mapping", `{"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [status]","caused_by":{"type":"illegal_argument_exception","reason":"expected object"},"root_cause":[{"type":"mapping_exception","reason":"conflicting field types"}],"stack_trace":"private-stack"}}`, []string{"mapper_parsing_exception", "failed to parse field [status]", "illegal_argument_exception", "expected object", "conflicting field types"}},
		{"string", `{"error":"index is read-only"}`, []string{"index is read-only"}},
		{"redaction", `{"error":{"type":"security_exception","reason":"test-secret Basic ` + base64.StdEncoding.EncodeToString([]byte("elastic:test-secret")) + `\naccess denied"},"request":"private-payload"}`, []string{"security_exception", "access denied", "[REDACTED]"}},
		{"non-json", "private-payload", []string{"unavailable"}},
		{"credentials", `{"error":"https://proxy:proxy-secret@example.com Bearer bearer-secret ApiKey api-secret"}`, []string{"[REDACTED]"}},
		{"detail-limit", `{"error":{"reason":"` + strings.Repeat("x", 3000) + `"}}`, []string{"truncated"}},
		{"long", `{"error":{"reason":"` + strings.Repeat("x", 20000) + `"}}`, []string{"truncated"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			srv := newTestESServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events", Username: "elastic", PasswordEnv: "TEST_ES_PASSWORD"}, "cluster")
			if err != nil {
				t.Fatal(err)
			}
			err = c.Publish(context.Background(), "ADDED", testutil.ExecutorPod("executor", "app"), false)
			if err == nil {
				t.Fatal("expected write error")
			}
			message := err.Error()
			for _, want := range append(tc.want, "HTTP 400", "index=events", "attempt=1/3", `username="elastic"`, `password_env="TEST_ES_PASSWORD"`, "password_empty=false") {
				if !strings.Contains(message, want) {
					t.Errorf("missing %q: %s", want, message)
				}
			}
			for _, secret := range []string{"test-secret", base64.StdEncoding.EncodeToString([]byte("elastic:test-secret")), "proxy-secret", "bearer-secret", "api-secret", "private-payload", "private-stack", "\n"} {
				if strings.Contains(message, secret) {
					t.Errorf("unsafe error detail: %q", message)
				}
			}
			if len(message) > 4096 || calls != 1 {
				t.Errorf("error size=%d, calls=%d", len(message), calls)
			}
		})
	}
}

func TestPublishTransportErrorDetails(t *testing.T) {
	srv := newTestESServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()
	c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events"}, "cluster")
	if err != nil {
		t.Fatal(err)
	}
	err = c.Publish(context.Background(), "ADDED", testutil.ExecutorPod("executor", "app"), false)
	if err == nil {
		t.Fatal("expected connection error")
	}
	for _, want := range []string{"connection refused", "index=events", "attempt=3/3", `username=""`, "password_empty=true"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q: %v", want, err)
		}
	}
}

func TestClientValidation(t *testing.T) {
	t.Setenv("TEST_ES_MISSING", "")
	for _, cfg := range []config.ElasticsearchConfig{
		{Address: "invalid", Index: "events"},
		{Address: "http://user:secret@localhost:9200", Index: "events"},
		{Address: "http://localhost:9200", Index: ""},
		{Address: "http://localhost:9200", Index: "INVALID"},
		{Address: "http://localhost:9200", Index: "events", Username: "elastic", PasswordEnv: "TEST_ES_MISSING"},
	} {
		if _, err := NewClient(cfg, "cluster"); err == nil {
			t.Error("expected invalid configuration to fail")
		}
	}
}

func TestPasswordValidationExplanation(t *testing.T) {
	t.Setenv("TEST_ES_EMPTY", "")
	t.Setenv("TEST_ES_UNSET", "temporary")
	if err := os.Unsetenv("TEST_ES_UNSET"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		env  string
		want string
	}{
		{"missing variable name", "", "set elasticsearch.passwordEnv to the environment variable name"},
		{"unset variable", "TEST_ES_UNSET", "environment variable is not set"},
		{"empty variable", "TEST_ES_EMPTY", "environment variable is empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewClient(config.ElasticsearchConfig{Address: "http://localhost:9200", Index: "events", Username: "elastic", PasswordEnv: tc.env}, "cluster")
			if err == nil {
				t.Fatal("expected missing password error")
			}
			for _, want := range []string{tc.want, `username="elastic"`, "password_empty=true", "passwordEnv is a variable name, not the password"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("missing %q: %v", want, err)
				}
			}
		})
	}
}

func TestLifecycleDocumentIDsAndNoPodSpec(t *testing.T) {
	var paths []string
	srv := newTestESServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if _, exists := body["spec"]; exists {
			t.Error("pod spec must not be exported")
		}
		if _, exists := body["status"]; exists {
			t.Error("top-level pod status must not be exported")
		}
		var metadata map[string]json.RawMessage
		if err := json.Unmarshal(body["metadata"], &metadata); err != nil {
			t.Error(err)
		}
		if _, exists := metadata["managedFields"]; exists {
			t.Error("metadata.managedFields must not be exported")
		}
		encoded, _ := json.Marshal(body)
		if strings.Contains(string(encoded), "private-env-value") {
			t.Error("environment leaked into event")
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events"}, "cluster")
	if err != nil {
		t.Fatal(err)
	}
	pod := testutil.WithUID(testutil.ExecutorPod("executor", "app"), "uid")
	pod.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "test-manager", Operation: metav1.ManagedFieldsOperationApply, APIVersion: "v1"}}
	pod.ResourceVersion = "123"
	pod.Spec.Containers = []corev1.Container{{Name: "main", Env: []corev1.EnvVar{{Name: "PASSWORD", Value: "private-env-value"}}}}
	for _, event := range []struct {
		kind    string
		initial bool
	}{{"ADDED", false}, {"ADDED", false}, {"ADDED", true}, {"DELETED", false}} {
		if err := c.Publish(context.Background(), event.kind, pod, event.initial); err != nil {
			t.Fatal(err)
		}
	}
	if paths[0] != paths[1] || paths[0] == paths[2] || paths[0] == paths[3] || paths[2] == paths[3] {
		t.Fatalf("incorrect document IDs: %v", paths)
	}
	if len(pod.ManagedFields) != 1 {
		t.Fatal("publishing must not modify the original Pod metadata")
	}
}

func TestPublishOmitsManagedFields(t *testing.T) {
	pod := testutil.ExecutorPod("executor", "app")
	pod.Annotations = map[string]string{"example.com/note": "keep"}
	pod.ManagedFields = []metav1.ManagedFieldsEntry{{
		Manager: "kubelet", FieldsType: "FieldsV1",
		FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:labels":{".":{},"f:spark-role":{}}}}`)},
	}}
	calls := 0
	srv := newTestESServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var document struct {
			Metadata map[string]json.RawMessage `json:"metadata"`
		}
		if err := json.NewDecoder(r.Body).Decode(&document); err != nil {
			t.Error(err)
		}
		if _, exists := document.Metadata["managedFields"]; exists {
			t.Error("managedFields must be omitted")
		}
		for _, field := range []string{"name", "namespace", "labels", "annotations"} {
			got, err := json.Marshal(document.Metadata[field])
			original, _ := json.Marshal(pod.ObjectMeta)
			var metadata map[string]json.RawMessage
			_ = json.Unmarshal(original, &metadata)
			if err != nil || string(got) != string(metadata[field]) {
				t.Errorf("metadata field %s changed", field)
			}
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events"}, "cluster")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"ADDED", "MODIFIED", "DELETED"} {
		if err := c.Publish(context.Background(), kind, pod, false); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 3 || len(pod.ManagedFields) != 1 || pod.ManagedFields[0].FieldsV1 == nil {
		t.Fatal("publishing must preserve the original pod's managedFields")
	}
}

func TestPublishPodMetricsDocument(t *testing.T) {
	var paths []string
	srv := newTestESServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		var doc PodMetrics
		if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
			t.Error(err)
		}
		if doc.Type != "POD_METRICS" || doc.Cluster != "cluster" || doc.ApplicationID != "app-e" || doc.Metrics.PodName != "app-e-driver" {
			t.Errorf("incorrect metrics document: %+v", doc)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events"}, "cluster")
	if err != nil {
		t.Fatal(err)
	}
	doc := podmetrics.PodMetricsDocument{ApplicationID: "app-e", Namespace: "default", PodName: "app-e-driver"}
	if err := c.PublishPodMetrics(context.Background(), doc); err != nil {
		t.Fatal(err)
	}
	doc.Metrics = map[string][]prometheus.Result{"cpu": {{Metric: map[string]string{"container": "main"}}}}
	if err := c.PublishPodMetrics(context.Background(), doc); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || paths[0] != paths[1] {
		t.Fatalf("document id must be stable per application: %v", paths)
	}
}

func newTestESServer(handler http.Handler) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		handler.ServeHTTP(w, r)
	}))
}

func TestSanitizeDocument(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
	}{
		{"nested objects and arrays", `{".":1,"..":2,"nested":{"...":3,"a.b":"keep","a.":4,".a":5},"items":[{".":0,"ok":true},[{"..":0}]],"value":"..."}`, `{"items":[{"ok":true},[{}]],"nested":{".a":5,"a.":4,"a.b":"keep"},"value":"..."}`},
		{"preserve numbers and empty keys", `{"id":9007199254740993,"":null,"fraction":1.25,"array":[1,".",null]}`, `{"":null,"array":[1,".",null],"fraction":1.25,"id":9007199254740993}`},
		{"invalid JSON", `{"broken":`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sanitizeDocument([]byte(tc.body))
			if tc.want == "" {
				if err == nil {
					t.Fatal("expected invalid JSON error")
				}
				return
			}
			if err != nil || string(got) != tc.want {
				t.Fatalf("got %s, error %v; want %s", got, err, tc.want)
			}
		})
	}
}
