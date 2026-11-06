package controller

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"podwatcher/internal/prometheus"

	"github.com/gin-gonic/gin"
)

// PrometheusClient is the subset of prometheus.Client exposed to the controller.
type PrometheusClient interface {
	QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration) ([]prometheus.Result, error)
}

// PrometheusGateway proxies Prometheus HTTP API requests, parsing Prometheus
// query string parameters and forwarding them to the underlying client.
type PrometheusGateway struct {
	client PrometheusClient
}

func NewPrometheusGateway(client PrometheusClient) *PrometheusGateway {
	return &PrometheusGateway{client: client}
}

// QueryRange guards the gateway:
//   - query: max 4KB PromQL
//   - start/end: required, end >= start, range <= 31d
//   - step: required, between 1s and 24h
//
// These limits exist to prevent accidental or hostile workloads from
// exhausting podwatcher memory (large response body) or stalling the
// Prometheus engine (long range × tiny step).
func (g *PrometheusGateway) QueryRange(ctx *gin.Context) {
	query := ctx.Query("query")
	if query == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "query is required"})
		return
	}
	if len(query) > maxQueryLen {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "query too long: max " + strconv.Itoa(maxQueryLen) + " bytes"})
		return
	}

	start, err := parseTime(ctx.Query("start"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid start: " + err.Error()})
		return
	}
	end, err := parseTime(ctx.Query("end"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid end: " + err.Error()})
		return
	}
	if end.Before(start) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "end must be >= start"})
		return
	}
	if end.Sub(start) > maxRange {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "range too large: max " + maxRange.String()})
		return
	}

	step, err := parseStep(ctx.Query("step"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid step: " + err.Error()})
		return
	}
	if step < minStep {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "step too small: min " + minStep.String()})
		return
	}
	if step > maxStep {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "step too large: max " + maxStep.String()})
		return
	}
	// Cap the number of samples per series to avoid pathological queries
	// forcing Prometheus to materialize huge matrices.
	if end.Sub(start)/step > maxPointsPerSeries {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "too many data points: increase step or narrow the range"})
		return
	}

	results, err := g.client.QueryRange(ctx.Request.Context(), query, start, end, step)
	if err != nil {
		ctx.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"resultType": "matrix",
			"result":     results,
		},
	})
}

const (
	maxQueryLen        = 4 * 1024
	maxRange           = 31 * 24 * time.Hour
	minStep            = 1 * time.Second
	maxStep            = 24 * time.Hour
	maxPointsPerSeries = 11000 // Prometheus default query.max_samples
)

// parseTime accepts RFC3339 or a fractional Unix timestamp (seconds).
func parseTime(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, errors.New("required")
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC(), nil
	}
	// Fractional Unix seconds.
	var sec float64
	if _, err := strconv.ParseFloat(raw, 64); err == nil {
		sec, _ = strconv.ParseFloat(raw, 64)
		return time.Unix(0, int64(sec*1e9)).UTC(), nil
	}
	return time.Time{}, errors.New("must be RFC3339 or Unix seconds")
}

// parseStep accepts Go duration strings ("30s", "1m", "5m30s") or a bare
// number interpreted as seconds.
func parseStep(raw string) (time.Duration, error) {
	if raw == "" {
		return 0, errors.New("required")
	}
	if d, err := time.ParseDuration(raw); err == nil {
		return d, nil
	}
	var sec float64
	if _, err := strconv.ParseFloat(raw, 64); err == nil {
		sec, _ = strconv.ParseFloat(raw, 64)
		return time.Duration(sec * float64(time.Second)), nil
	}
	return 0, errors.New("must be Go duration (e.g. 30s) or seconds")
}
