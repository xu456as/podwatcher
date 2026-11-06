package prometheus

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Config configures the Prometheus HTTP API client. Address and Token are
// expected to be fully resolved by the caller — the client does not read
// environment variables itself.
type Config struct {
	Address            string `yaml:"address"`            // Prometheus base URL, e.g. http://prometheus:9090
	Token              string `yaml:"token"`              // resolved bearer token
	TimeoutSeconds     int    `yaml:"timeoutSeconds"`     // per-request timeout in seconds
	InsecureSkipVerify bool   `yaml:"insecureSkipVerify"` // skip TLS verification for self-signed endpoints
}

// Sample is a single timestamp/value pair returned by Prometheus.
type Sample struct {
	Timestamp float64
	Value     string
}

// UnmarshalJSON parses the [timestamp, "value"] array form used by Prometheus.
func (s *Sample) UnmarshalJSON(data []byte) error {
	var pair []json.RawMessage
	if err := json.Unmarshal(data, &pair); err != nil || len(pair) != 2 {
		return fmt.Errorf("invalid sample: %s", string(data))
	}
	var ts float64
	if err := json.Unmarshal(pair[0], &ts); err != nil {
		return fmt.Errorf("invalid sample timestamp: %w", err)
	}
	// Prometheus returns values as JSON strings; fall back to number for safety.
	var val string
	if err := json.Unmarshal(pair[1], &val); err != nil {
		var num float64
		if err := json.Unmarshal(pair[1], &num); err != nil {
			return fmt.Errorf("invalid sample value: %s", string(pair[1]))
		}
		val = strconv.FormatFloat(num, 'f', -1, 64)
	}
	s.Timestamp = ts
	s.Value = val
	return nil
}

// Result is one series in a Prometheus query response. Range queries populate
// Values; instant queries populate Value.
type Result struct {
	Metric map[string]string `json:"metric"`
	Value  *Sample           `json:"value,omitempty"`
	Values []Sample          `json:"values,omitempty"`
}

// Response is the envelope returned by the Prometheus HTTP API.
type Response struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string   `json:"resultType"`
		Result     []Result `json:"result"`
	} `json:"data"`
	Error    string   `json:"error,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// Client queries the Prometheus HTTP API.
type Client struct {
	timeout    time.Duration
	base       string
	token      string
	authInfo   string
	redact     *strings.Replacer
	httpClient *http.Client
}

// NewClient builds a Prometheus API client. Address and Token must be fully
// resolved by the caller — this function does not read the environment.
func NewClient(cfg Config) *Client {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	token := cfg.Token
	var replacements []string
	if token != "" {
		for _, secret := range []string{token, "Bearer " + token, url.QueryEscape(token), url.PathEscape(token)} {
			replacements = append(replacements, secret, "[REDACTED]")
		}
	}
	// Dedicated transport with explicit connection pool sizing; defaults from
	// http.DefaultTransport leave MaxIdleConnsPerHost at 2 which starves
	// concurrent gateway requests.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 32
	transport.MaxConnsPerHost = 64
	transport.IdleConnTimeout = 90 * time.Second
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify}
	return &Client{
		timeout:    timeout,
		base:       strings.TrimRight(cfg.Address, "/"),
		token:      token,
		authInfo:   fmt.Sprintf("token_set=%t", token != ""),
		redact:     strings.NewReplacer(replacements...),
		httpClient: &http.Client{Transport: transport},
	}
}

// QueryRange performs an /api/v1/query_range request returning a matrix of
// samples over the [start, end] interval at the given step. Retries 5xx and
// 429 responses up to 3 times with linear backoff.
func (c *Client) QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration) ([]Result, error) {
	values := url.Values{}
	values.Set("query", query)
	values.Set("start", formatTime(start))
	values.Set("end", formatTime(end))
	values.Set("step", formatStep(step))
	return c.do(ctx, "/api/v1/query_range", values)
}

// Query performs an /api/v1/query instant vector request at ts. If ts is zero,
// the server evaluates at the current time.
func (c *Client) Query(ctx context.Context, query string, ts time.Time) ([]Result, error) {
	values := url.Values{}
	values.Set("query", query)
	if !ts.IsZero() {
		values.Set("time", formatTime(ts))
	}
	return c.do(ctx, "/api/v1/query", values)
}

// formatTime renders a timestamp as a fractional Unix second value, the form
// Prometheus accepts for start/end/time parameters.
func formatTime(t time.Time) string {
	return strconv.FormatFloat(float64(t.UnixNano())/1e9, 'f', -1, 64)
}

func formatStep(step time.Duration) string {
	return strconv.FormatFloat(step.Seconds(), 'f', -1, 64)
}

func (c *Client) do(ctx context.Context, path string, values url.Values) (results []Result, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w (%s)", err, c.authInfo)
		}
	}()
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
			}
		}
		attemptCtx, cancel := context.WithTimeout(ctx, c.timeout)
		retriable, requestErr := c.request(attemptCtx, path, values, attempt, &results)
		cancel()
		if requestErr == nil {
			return results, nil
		}
		if !retriable {
			return nil, requestErr
		}
		err = requestErr
	}
	return nil, err
}

// request executes a single HTTP attempt. The retriable flag indicates whether
// a failure (transport error or 5xx/429) should be retried by the caller.
func (c *Client) request(ctx context.Context, path string, values url.Values, attempt int, out *[]Result) (retriable bool, err error) {
	reqURL := c.base + path + "?" + values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return false, fmt.Errorf("build Prometheus request (path=%s attempt=%d/3): %s", path, attempt+1, c.safeDetail(err.Error()))
	}
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, requestErr := c.httpClient.Do(req)
	if requestErr != nil {
		// Strip request URLs (which may contain credentials or query tokens),
		// while retaining DNS, connection, TLS and timeout diagnostics.
		var urlErr *url.Error
		if errors.As(requestErr, &urlErr) {
			requestErr = urlErr.Err
		}
		return true, fmt.Errorf("Prometheus request failed (path=%s attempt=%d/3): %s", path, attempt+1, c.safeDetail(requestErr.Error()))
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// Success responses can be large (many series × many data points).
		// Cap at maxSuccessBody to protect memory; legitimate query_range
		// responses fit well under this with sane step/interval.
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxSuccessBody+1))
		if readErr != nil {
			return false, fmt.Errorf("read Prometheus response (path=%s attempt=%d/3): %s", path, attempt+1, readErr.Error())
		}
		if len(body) > maxSuccessBody {
			return false, fmt.Errorf("Prometheus response too large (path=%s attempt=%d/3): exceeds %d bytes; narrow the query, shorten the range, or increase the step", path, attempt+1, maxSuccessBody)
		}
		var response Response
		if jsonErr := json.Unmarshal(body, &response); jsonErr != nil {
			return false, fmt.Errorf("decode Prometheus response (path=%s attempt=%d/3): %s; body=%s", path, attempt+1, jsonErr.Error(), c.safeDetail(string(body)))
		}
		if response.Status != "success" {
			return false, fmt.Errorf("Prometheus query failed (path=%s attempt=%d/3): status=%s error=%s warnings=%s", path, attempt+1, response.Status, response.Error, strings.Join(response.Warnings, "; "))
		}
		*out = response.Data.Result
		return false, nil
	}
	// Error responses are typically small — cap the body to avoid log spam.
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody+1))
	detail := c.safeDetail(string(body))
	if readErr != nil {
		detail += "; reading error response: " + readErr.Error()
	}
	err = fmt.Errorf("Prometheus returned HTTP %d (path=%s attempt=%d/3): %s", resp.StatusCode, path, attempt+1, detail)
	return resp.StatusCode == 429 || resp.StatusCode >= 500, err
}

const (
	maxErrorBody   = 16 * 1024
	maxSuccessBody = 64 * 1024 * 1024 // 64MB — protects memory from pathological query_range responses
)

var (
	urlCredentials  = regexp.MustCompile(`(?i)(https?://)[^\s/@]+@`)
	authCredentials = regexp.MustCompile(`(?i)\b(Basic|Bearer|ApiKey)\s+[^\s,;"']+`)
)

// safeDetail strips credentials and escapes control characters so external
// error messages stay on one log line and never leak secrets.
func (c *Client) safeDetail(detail string) string {
	detail = urlCredentials.ReplaceAllString(detail, "${1}[REDACTED]@")
	detail = authCredentials.ReplaceAllString(detail, "${1} [REDACTED]")
	detail = c.redact.Replace(detail)
	detail = strconv.QuoteToASCII(detail)
	if len(detail) > 2048 {
		detail = detail[:2048] + "... [truncated]"
	}
	return detail
}
