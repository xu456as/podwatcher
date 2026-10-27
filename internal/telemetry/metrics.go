package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// InitMeter sets up the global MeterProvider exporting metrics via OTLP/HTTP
// to the same collector endpoint as traces. An empty endpoint disables metrics
// and returns a no-op shutdown function. The returned shutdown function flushes
// pending metrics and must be called on exit.
func InitMeter(ctx context.Context, endpoint, serviceName string) (func(context.Context) error, error) {
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	opts := []otlpmetrichttp.Option{
		otlpmetrichttp.WithEndpoint(trimScheme(endpoint)),
	}
	if !isSecure(endpoint) {
		opts = append(opts, otlpmetrichttp.WithInsecure())
	}

	exporter, err := otlpmetrichttp.New(ctx, opts...)
	if err != nil {
		return nil, err
	}

	mp := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(exporter)),
		metric.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		)),
	)
	otel.SetMeterProvider(mp)

	return mp.Shutdown, nil
}
