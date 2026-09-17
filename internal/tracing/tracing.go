// Package tracing is orbit's only place that imports the OpenTelemetry SDK
// directly -- same containment principle as internal/metrics for
// Prometheus, with the same documented exception: the OpenTelemetry API
// (as opposed to the SDK setup this package owns) is designed to be called
// from anywhere once a global TracerProvider is installed, the same way
// internal/metrics's counters are meant to be incremented directly from
// business logic. This package's actual job is narrower than that: wire up
// the OTLP exporter and install the global provider once, at startup.
package tracing

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// Init connects to an OTLP collector (Jaeger, in this project's compose
// stack, which has accepted OTLP natively since it deprecated its own
// Thrift wire format) at endpoint, installs a global TracerProvider
// tagged with serviceName, and sets the global W3C trace-context
// propagator -- the thing that actually knows how to serialize a span's
// identity into a string and back, which internal/queue's Kafka header
// carrier depends on.
//
// The returned shutdown func flushes any buffered spans and closes the
// exporter connection; callers should defer it. If Init itself fails
// (e.g. the collector is unreachable), it returns a no-op shutdown and an
// error -- callers are expected to log it and continue without tracing,
// same "a peripheral concern shouldn't take down the core loop" reasoning
// as internal/metrics.Serve's bind failure and the rate limiter's
// fail-open behavior. A missing trace of one run is a worse outcome to
// avoid turning into "the scheduler won't start."
func Init(ctx context.Context, serviceName, endpoint string) (shutdown func(context.Context) error, err error) {
	noop := func(context.Context) error { return nil }

	connCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	exporter, err := otlptracegrpc.New(connCtx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(), // local dev only -- see README
	)
	if err != nil {
		return noop, fmt.Errorf("tracing: connect to %s: %w", endpoint, err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return noop, fmt.Errorf("tracing: build resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	// The W3C Trace Context propagator is what turns a span's (trace ID,
	// span ID) into the "traceparent" string format and back -- the exact
	// mechanism internal/queue's Kafka header carrier uses to serialize a
	// span across a message boundary instead of an HTTP header, which is
	// the only place this format usually shows up.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}
