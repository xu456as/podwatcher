package elasticsearch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"podwatcher/internal/config"
	"podwatcher/internal/podmetrics"
	"podwatcher/internal/store"

	es "github.com/elastic/go-elasticsearch/v8"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Event preserves pod metadata without exporting the pod spec,
// which can contain environment credentials.
type Event struct {
	ApplicationID string            `json:"applicationId"`
	PodName       string            `json:"podName"`
	Queue         string            `json:"queue"`
	Type          string            `json:"type"`
	Cluster       string            `json:"cluster"`
	Timestamp     time.Time         `json:"@timestamp"`
	Initial       bool              `json:"isInitialSnapshot"`
	AddedAt       *time.Time        `json:"addedAt,omitempty"`
	DeletedAt     *time.Time        `json:"deletedAt,omitempty"`
	Metadata      metav1.ObjectMeta `json:"metadata"`
	Node          string            `json:"node"`
}

type Client struct {
	timeout      time.Duration
	sdk          *es.Client
	index        string
	metricsIndex string
	cluster      string
	redact       *strings.Replacer
	authInfo     string
}

// Option customizes a Client after the base Elasticsearch configuration has
// been validated.
type Option func(*Client) error

// WithPodMetricsIndex routes POD_METRICS documents to their own index so the
// label-heavy metrics mapping does not share the lifecycle event mapping.
func WithPodMetricsIndex(index string) Option {
	return func(c *Client) error {
		if index == "" {
			return nil // keep the event index
		}
		if err := validateIndexName("podMetricsExport.index", index); err != nil {
			return err
		}
		c.metricsIndex = index
		return nil
	}
}

// validateIndexName rejects names Elasticsearch cannot use as an index.
func validateIndexName(field, index string) error {
	if index == "" || strings.ContainsAny(index, "/\\ *?\"<>|,#:") || strings.ToLower(index) != index || strings.HasPrefix(index, "_") || strings.HasPrefix(index, "-") || strings.HasPrefix(index, "+") || index == "." || index == ".." {
		return fmt.Errorf("invalid %s", field)
	}
	return nil
}

func NewClient(cfg config.ElasticsearchConfig, cluster string, opts ...Option) (*Client, error) {
	u, err := url.Parse(cfg.Address)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("elasticsearch.address must be an HTTP(S) URL without credentials, query or fragment")
	}
	if err := validateIndexName("elasticsearch.index", cfg.Index); err != nil {
		return nil, err
	}
	password, passwordSet := os.LookupEnv(cfg.PasswordEnv)
	authInfo := fmt.Sprintf("username=%q password_env=%q password_empty=%t", cfg.Username, cfg.PasswordEnv, password == "")
	if cfg.Username != "" && (cfg.PasswordEnv == "" || password == "") {
		reason := "environment variable is empty"
		if cfg.PasswordEnv == "" {
			reason = "set elasticsearch.passwordEnv to the environment variable name"
		} else if !passwordSet {
			reason = "environment variable is not set"
		}
		return nil, fmt.Errorf("Elasticsearch authentication configuration invalid (%s): %s; inject a non-empty password into the container environment; passwordEnv is a variable name, not the password", authInfo, reason)
	}
	if cfg.TimeoutSeconds < 0 || int64(cfg.TimeoutSeconds) > int64((1<<63-1)/time.Second) {
		return nil, fmt.Errorf("invalid elasticsearch.timeoutSeconds: use positive seconds")
	}
	if cfg.TimeoutSeconds == 0 {
		cfg.TimeoutSeconds = 20
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify} // Explicit configuration for self-signed ES endpoints.
	sdk, err := es.NewClient(es.Config{
		Transport:               transport,
		Addresses:               []string{cfg.Address},
		Username:                cfg.Username,
		Password:                password,
		EnableCompatibilityMode: true,
		// The existing write loop owns retry policy and per-attempt timeouts.
		DisableRetry: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Elasticsearch SDK (%s)", authInfo)
	}
	var replacements []string
	if password != "" {
		for _, secret := range []string{base64.StdEncoding.EncodeToString([]byte(cfg.Username + ":" + password)), password, url.QueryEscape(password), url.PathEscape(password)} {
			replacements = append(replacements, secret, "[REDACTED]")
		}
	}
	client := &Client{
		sdk: sdk, index: cfg.Index, metricsIndex: cfg.Index, cluster: cluster,
		timeout: time.Duration(cfg.TimeoutSeconds) * time.Second,
		redact:  strings.NewReplacer(replacements...), authInfo: authInfo,
	}
	for _, opt := range opts {
		if err := opt(client); err != nil {
			return nil, err
		}
	}
	return client, nil
}

// Publish uses a stable id so retries and watch replays overwrite the same event.
// INITIAL adds have their own id and cannot replace a real ADDED event.
func (c *Client) Publish(ctx context.Context, eventType string, pod *corev1.Pod, initial bool) error {
	identity := string(pod.UID)
	if identity == "" {
		identity = pod.Namespace + "/" + pod.Name
	}
	idBytes, _ := json.Marshal([]interface{}{c.cluster, identity, pod.ResourceVersion, eventType, initial})
	hash := sha256.Sum256(idBytes)
	id := hex.EncodeToString(hash[:])
	now := time.Now().UTC()
	event := Event{ApplicationID: store.AppIDFromPod(pod), PodName: pod.Name, Queue: pod.Labels["queue"], Type: eventType, Cluster: c.cluster, Timestamp: now, Initial: initial, Metadata: pod.ObjectMeta, Node: pod.Spec.NodeName}
	// Strip field ownership bookkeeping from the document's metadata copy.
	event.Metadata.ManagedFields = nil
	if !pod.CreationTimestamp.IsZero() {
		addedAt := pod.CreationTimestamp.Time.UTC()
		event.AddedAt = &addedAt
	}
	if eventType == "DELETED" {
		event.DeletedAt = &now
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return c.write(ctx, c.index, id, body)
}

// Completion is distinct from Pod events and keeps status under application.
type Completion struct {
	ApplicationID string                `json:"applicationId"`
	PodName       string                `json:"podName"`
	Queue         string                `json:"queue"`
	Type          string                `json:"type"`
	Cluster       string                `json:"cluster"`
	Timestamp     time.Time             `json:"@timestamp"`
	IndexedAt     time.Time             `json:"indexedAt"`
	FinishedAt    time.Time             `json:"finishedAt"`
	Application   store.ApplicationView `json:"application"`
}

func (c *Client) PublishApplication(ctx context.Context, app store.ApplicationView) error {
	if app.FinishedAt == nil {
		return fmt.Errorf("application has no finishedAt")
	}
	identity, _ := json.Marshal([]string{c.cluster, app.Namespace, app.ApplicationID, "APPLICATION_COMPLETED"})
	hash := sha256.Sum256(identity)
	now := time.Now().UTC()
	app.Cluster = c.cluster
	// Deletion after success/failure must retain the actual completion outcome.
	if app.DriverPhase == string(corev1.PodSucceeded) {
		app.Status = "succeeded"
	}
	if app.DriverPhase == string(corev1.PodFailed) {
		app.Status = "failed"
	}
	body, err := json.Marshal(Completion{ApplicationID: app.ApplicationID, PodName: app.DriverPodName, Queue: app.DriverLabels["queue"], Type: "APPLICATION_COMPLETED", Cluster: c.cluster, Timestamp: now, IndexedAt: now, FinishedAt: *app.FinishedAt, Application: app})
	if err != nil {
		return err
	}
	return c.write(ctx, c.index, hex.EncodeToString(hash[:]), body)
}

// PodMetrics is distinct from Pod events and Completion: it carries the
// aggregated Prometheus metrics of one driver pod's lifecycle under metrics.
type PodMetrics struct {
	ApplicationID string                        `json:"applicationId"`
	PodName       string                        `json:"podName"`
	Type          string                        `json:"type"`
	Cluster       string                        `json:"cluster"`
	Timestamp     time.Time                     `json:"@timestamp"`
	IndexedAt     time.Time                     `json:"indexedAt"`
	Metrics       podmetrics.PodMetricsDocument `json:"metrics"`
}

// PublishPodMetrics indexes one Spark pod's aggregated lifecycle metrics.
// The document id is stable per application so retries and re-delivered
// delete events overwrite instead of duplicating.
func (c *Client) PublishPodMetrics(ctx context.Context, doc podmetrics.PodMetricsDocument) error {
	identity, _ := json.Marshal([]string{c.cluster, doc.Namespace, doc.ApplicationID, "POD_METRICS"})
	hash := sha256.Sum256(identity)
	now := time.Now().UTC()
	body, err := json.Marshal(PodMetrics{
		ApplicationID: doc.ApplicationID,
		PodName:       doc.PodName,
		Type:          "POD_METRICS",
		Cluster:       c.cluster,
		Timestamp:     now,
		IndexedAt:     now,
		Metrics:       doc,
	})
	if err != nil {
		return err
	}
	return c.write(ctx, c.metricsIndex, hex.EncodeToString(hash[:]), body)
}

// PublishError carries the actual sanitized document for debug logging only.
// Error deliberately excludes the payload from normal error logs.
type PublishError struct {
	Err     error
	payload string
}

func (e *PublishError) Error() string        { return e.Err.Error() }
func (e *PublishError) Unwrap() error        { return e.Err }
func (e *PublishError) DebugPayload() string { return e.payload }

func (c *Client) write(ctx context.Context, index, id string, body []byte) (err error) {
	body, err = sanitizeDocument(body)
	if err != nil {
		return fmt.Errorf("sanitize Elasticsearch document: %w", err)
	}
	defer func() {
		if err != nil {
			err = &PublishError{Err: fmt.Errorf("%w (%s)", err, c.authInfo), payload: c.redact.Replace(string(body))}
		}
	}()
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
			}
		}
		attemptCtx, cancel := context.WithTimeout(ctx, c.timeout)
		resp, requestErr := c.sdk.Index(index, bytes.NewReader(body), c.sdk.Index.WithDocumentID(id), c.sdk.Index.WithContext(attemptCtx))
		if requestErr != nil {
			cancel()
			// Strip request URLs (which may contain credentials or query tokens),
			// while retaining DNS, connection, TLS and timeout diagnostics.
			var urlErr *url.Error
			if errors.As(requestErr, &urlErr) {
				requestErr = urlErr.Err
			}
			err = fmt.Errorf("Elasticsearch request failed (index=%s attempt=%d/3): %s", index, attempt+1, c.safeDetail(requestErr.Error()))
			continue
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody+1))
		resp.Body.Close()
		cancel()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		detail := responseErrorDetail(responseBody)
		if readErr != nil {
			detail += "; reading error response: " + readErr.Error()
		}
		err = fmt.Errorf("Elasticsearch returned HTTP %d (index=%s attempt=%d/3): %s", resp.StatusCode, index, attempt+1, c.safeDetail(detail))
		if resp.StatusCode != 429 && resp.StatusCode < 500 {
			return err
		}
	}
	return err
}

// sanitizeDocument removes dot-only field names throughout objects and arrays.
// UseNumber preserves integer precision when re-encoding the document.
func sanitizeDocument(body []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var document interface{}
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	removeDotOnlyKeys(document)
	return json.Marshal(document)
}

func removeDotOnlyKeys(value interface{}) {
	switch value := value.(type) {
	case map[string]interface{}:
		for key, child := range value {
			if key != "" && strings.Trim(key, ".") == "" {
				delete(value, key)
				continue
			}
			removeDotOnlyKeys(child)
		}
	case []interface{}:
		for _, child := range value {
			removeDotOnlyKeys(child)
		}
	}
}

const maxErrorBody = 16 * 1024

// Only extract diagnostic fields, never the entire response, headers or document.
func responseErrorDetail(body []byte) string {
	if len(body) > maxErrorBody {
		return "error details unavailable (response truncated)"
	}
	var envelope struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(body, &envelope) != nil || len(envelope.Error) == 0 {
		return "error details unavailable (empty or non-ES JSON response)"
	}
	detail := describeESError(envelope.Error, 0)
	if detail == "" {
		return "error details unavailable"
	}
	return detail
}

func describeESError(raw json.RawMessage, depth int) string {
	if depth >= 4 {
		return "nested error truncated"
	}
	var message string
	if json.Unmarshal(raw, &message) == nil {
		return message
	}
	var detail struct {
		Type      string            `json:"type"`
		Reason    string            `json:"reason"`
		RootCause []json.RawMessage `json:"root_cause"`
		CausedBy  json.RawMessage   `json:"caused_by"`
	}
	if json.Unmarshal(raw, &detail) != nil {
		return ""
	}
	var parts []string
	if detail.Type != "" {
		parts = append(parts, "type="+detail.Type)
	}
	if detail.Reason != "" {
		parts = append(parts, "reason="+detail.Reason)
	}
	for _, root := range detail.RootCause {
		if value := describeESError(root, depth+1); value != "" {
			parts = append(parts, "root_cause={"+value+"}")
		}
	}
	if len(detail.CausedBy) > 0 {
		if value := describeESError(detail.CausedBy, depth+1); value != "" {
			parts = append(parts, "caused_by={"+value+"}")
		}
	}
	return strings.Join(parts, "; ")
}

var (
	urlCredentials  = regexp.MustCompile(`(?i)(https?://)[^\s/@]+@`)
	authCredentials = regexp.MustCompile(`(?i)\b(Basic|Bearer|ApiKey)\s+[^\s,;"']+`)
)

func (c *Client) safeDetail(detail string) string {
	detail = urlCredentials.ReplaceAllString(detail, "${1}[REDACTED]@")
	detail = authCredentials.ReplaceAllString(detail, "${1} [REDACTED]")
	detail = c.redact.Replace(detail)
	// Escape control characters to keep external messages on one log line.
	detail = strconv.QuoteToASCII(detail)
	if len(detail) > 2048 {
		detail = detail[:2048] + "... [truncated]"
	}
	return detail
}
