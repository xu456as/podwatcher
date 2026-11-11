package elasticsearch

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"k8s.io/klog/v2"
)

var (
	metricsOnce      sync.Once
	writeStatusCodes otelmetric.Int64Counter
)

func ensureMetrics() {
	metricsOnce.Do(func() {
		meter := otel.Meter("podwatcher/elasticsearch")
		var err error
		if writeStatusCodes, err = meter.Int64Counter("podwatcher.elasticsearch.write_status_codes",
			otelmetric.WithDescription("Elasticsearch write attempts by caller and HTTP status code. Status 0 means no HTTP response (network/TLS/timeout failure).")); err != nil {
			klog.Warningf("Failed to create elasticsearch write_status_codes counter: %v", err)
		}
	})
}

// recordWriteStatus emits one count per write attempt. callers are the three
// business-level entry points: Publish, PublishApplication, PublishPodMetrics.
// status is the HTTP status code returned by Elasticsearch, or 0 when the SDK
// failed before getting a response (connection refused, TLS, timeout, ...).
func recordWriteStatus(ctx context.Context, caller string, status int) {
	ensureMetrics()
	if writeStatusCodes == nil {
		return
	}
	writeStatusCodes.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("caller", caller),
			attribute.Int("status_code", status),
		),
	)
}
