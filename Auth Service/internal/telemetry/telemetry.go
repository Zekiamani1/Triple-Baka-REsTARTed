package telemetry

import (
	"context"
	"errors"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const ScopeName = "auth-service"

func RecordRPC(ctx context.Context, method, statusCode string, durationMs float64) {
	m := otel.Meter(ScopeName)
	attrs := metric.WithAttributes(
		attribute.String("rpc.method", method),
		attribute.String("rpc.grpc.status_code", statusCode),
	)
	if c, err := m.Int64Counter(
		"auth.rpc.requests_total",
		metric.WithDescription("Total number of gRPC requests handled by Auth Service"),
	); err == nil {
		c.Add(ctx, 1, attrs)
	}
	if h, err := m.Float64Histogram(
		"auth.rpc.duration_ms",
		metric.WithDescription("Duration of gRPC requests in milliseconds"),
		metric.WithUnit("ms"),
	); err == nil {
		h.Record(ctx, durationMs, attrs)
	}
}

func RecordTokenIssued(ctx context.Context, grantType, clientID, scope string) {
	if c, err := otel.Meter(ScopeName).Int64Counter(
		"auth.tokens.issued_total",
		metric.WithDescription("Total number of access/refresh token pairs issued"),
	); err == nil {
		c.Add(ctx, 1, metric.WithAttributes(
			attribute.String("grant_type", grantType),
			attribute.String("client_id", clientID),
			attribute.String("scope", scope),
		))
	}
}

func RecordAuthFailure(ctx context.Context, operation, reason string) {
	if c, err := otel.Meter(ScopeName).Int64Counter(
		"auth.failures_total",
		metric.WithDescription("Total number of authentication or token refresh failures"),
	); err == nil {
		c.Add(ctx, 1, metric.WithAttributes(
			attribute.String("operation", operation),
			attribute.String("reason", reason),
		))
	}
}

func RecordJWKSRequest(ctx context.Context) {
	if c, err := otel.Meter(ScopeName).Int64Counter(
		"auth.jwks.requests_total",
		metric.WithDescription("Total number of JWKS endpoint requests"),
	); err == nil {
		c.Add(ctx, 1)
	}
}

func RecordDBQuery(ctx context.Context, operation string, durationMs float64) {
	if h, err := otel.Meter(ScopeName).Float64Histogram(
		"auth.db.query.duration_ms",
		metric.WithDescription("Duration of PostgreSQL database queries in milliseconds"),
		metric.WithUnit("ms"),
	); err == nil {
		h.Record(ctx, durationMs, metric.WithAttributes(
			attribute.String("db.operation", operation),
		))
	}
}

func Init(ctx context.Context, defaultServiceName string) (func(context.Context) error, error) {
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = defaultServiceName
	}

	res, err := resource.New(
		ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("1.0.0"),
			attribute.String("service.component", defaultServiceName),
		),
	)
	if err != nil {
		return nil, err
	}

	var traceOpts []otlptracegrpc.Option
	var metricOpts []otlpmetricgrpc.Option
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		traceOpts = append(traceOpts, otlptracegrpc.WithInsecure())
		metricOpts = append(metricOpts, otlpmetricgrpc.WithInsecure())
	}

	traceExporter, err := otlptracegrpc.New(ctx, traceOpts...)
	if err != nil {
		return nil, err
	}

	metricExporter, err := otlpmetricgrpc.New(ctx, metricOpts...)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(10*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func(shutdownCtx context.Context) error {
		return errors.Join(
			tp.Shutdown(shutdownCtx),
			mp.Shutdown(shutdownCtx),
		)
	}, nil
}
