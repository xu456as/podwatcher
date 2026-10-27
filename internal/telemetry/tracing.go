// Package telemetry initializes the OpenTelemetry trace SDK and exports
// spans over OTLP/HTTP to the configured collector endpoint.
package telemetry

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// trimScheme strips the http:// or https:// prefix from an endpoint URL.
func trimScheme(endpoint string) string {
	return strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")
}

// isSecure reports whether the endpoint URL uses an https:// scheme.
func isSecure(endpoint string) bool {
	return strings.HasPrefix(endpoint, "https://")
}

// endpointOptions converts an http(s):// endpoint URL into exporter options:
// the exporter expects a host:port target without a scheme; the scheme decides
// whether TLS is used (https) or not (http).
func endpointOptions(endpoint string) []otlptracehttp.Option {
	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(trimScheme(endpoint)),
	}
	if !isSecure(endpoint) {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	return opts
}

// InitTracer sets up the global TracerProvider exporting spans via OTLP/HTTP.
// An empty endpoint disables tracing and returns a no-op shutdown function.
// The returned shutdown function flushes pending spans and must be called on exit.
func InitTracer(ctx context.Context, endpoint, serviceName string) (func(context.Context) error, error) {
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracehttp.New(ctx, endpointOptions(endpoint)...)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		)),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}
