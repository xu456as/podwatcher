package store

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"

	"k8s.io/klog/v2"
)

// sizeBuckets spans compact to multi-hundred-megabyte serialized snapshots.
var sizeBuckets = []float64{64 * 1024, 256 * 1024, 1 << 20, 4 << 20, 16 << 20, 64 << 20, 256 << 20}

// Counters and gauges complement the measureFunc duration histogram with the
// signals it cannot express: how many records each eviction removed, the
// current store sizes (eviction pressure), and state-file persist outcomes.
// Like measureFunc they are created lazily on first use so they bind to the
// real MeterProvider that main installs in InitMeter; before that (and in
// tests without telemetry) they bind to the no-op default provider.
var (
	countersOnce      sync.Once
	appEvictions      otelmetric.Int64Counter
	podEvictions      otelmetric.Int64Counter
	podCascadeDeletes otelmetric.Int64Counter
	saveFailures      otelmetric.Int64Counter
	saveBytes         otelmetric.Int64Histogram
	applicationsGauge otelmetric.Int64Gauge
	podsGauge         otelmetric.Int64Gauge
)

func ensureStoreCounters() {
	countersOnce.Do(func() {
		meter := otel.Meter("podwatcher/store")
		var err error
		if appEvictions, err = meter.Int64Counter("podwatcher.store.app_evictions",
			otelmetric.WithDescription("Applications evicted (oldest deleted first) when over maxApplications")); err != nil {
			klog.Warningf("Failed to create store app_evictions counter: %v", err)
		}
		if podEvictions, err = meter.Int64Counter("podwatcher.store.pod_evictions",
			otelmetric.WithDescription("Pod records evicted (oldest deleted first) when over maxPodRecords")); err != nil {
			klog.Warningf("Failed to create store pod_evictions counter: %v", err)
		}
		if podCascadeDeletes, err = meter.Int64Counter("podwatcher.store.pod_cascade_deletes",
			otelmetric.WithDescription("Pod records removed together with their evicted application")); err != nil {
			klog.Warningf("Failed to create store pod_cascade_deletes counter: %v", err)
		}
		if saveFailures, err = meter.Int64Counter("podwatcher.store.save_failures",
			otelmetric.WithDescription("Failed state file persist attempts")); err != nil {
			klog.Warningf("Failed to create store save_failures counter: %v", err)
		}
		if saveBytes, err = meter.Int64Histogram("podwatcher.store.save_bytes",
			otelmetric.WithUnit("By"),
			otelmetric.WithDescription("Serialized state snapshot size"),
			otelmetric.WithExplicitBucketBoundaries(sizeBuckets...)); err != nil {
			klog.Warningf("Failed to create store save_bytes histogram: %v", err)
		}
		if applicationsGauge, err = meter.Int64Gauge("podwatcher.store.applications",
			otelmetric.WithDescription("Current application record count")); err != nil {
			klog.Warningf("Failed to create store applications gauge: %v", err)
		}
		if podsGauge, err = meter.Int64Gauge("podwatcher.store.pods",
			otelmetric.WithDescription("Current pod record count")); err != nil {
			klog.Warningf("Failed to create store pods gauge: %v", err)
		}
	})
}

func recordAppEvictions(n int) {
	ensureStoreCounters()
	if appEvictions != nil && n > 0 {
		appEvictions.Add(context.Background(), int64(n))
	}
}

func recordPodEvictions(n int) {
	ensureStoreCounters()
	if podEvictions != nil && n > 0 {
		podEvictions.Add(context.Background(), int64(n))
	}
}

func recordPodCascadeDeletes(n int) {
	ensureStoreCounters()
	if podCascadeDeletes != nil && n > 0 {
		podCascadeDeletes.Add(context.Background(), int64(n))
	}
}

func recordSaveFailure() {
	ensureStoreCounters()
	if saveFailures != nil {
		saveFailures.Add(context.Background(), 1)
	}
}

func recordSaveBytes(size int) {
	ensureStoreCounters()
	if saveBytes != nil {
		saveBytes.Record(context.Background(), int64(size))
	}
}

// recordStoreSizes publishes the current map sizes so eviction pressure
// (size hovering at maxApplications/maxPodRecords) is visible over time.
func recordStoreSizes(apps, pods int) {
	ensureStoreCounters()
	ctx := context.Background()
	if applicationsGauge != nil {
		applicationsGauge.Record(ctx, int64(apps))
	}
	if podsGauge != nil {
		podsGauge.Record(ctx, int64(pods))
	}
}
