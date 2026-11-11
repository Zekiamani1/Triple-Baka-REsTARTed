package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const bmkgPollerScope = "bmkg-poller"

type contextLogHandler struct {
	slog.Handler
	otelHandler slog.Handler
}

func newContextLogHandler(h slog.Handler) *contextLogHandler {
	return &contextLogHandler{
		Handler:     h,
		otelHandler: otelslog.NewHandler(bmkgPollerScope),
	}
}

func (h *contextLogHandler) Handle(ctx context.Context, record slog.Record) error {
	otelRecord := record.Clone()
	if spanCtx := trace.SpanContextFromContext(ctx); spanCtx.IsValid() {
		record.AddAttrs(
			slog.String("trace_id", spanCtx.TraceID().String()),
			slog.String("span_id", spanCtx.SpanID().String()),
		)
	}
	_ = h.otelHandler.Handle(ctx, otelRecord)
	return h.Handler.Handle(ctx, record)
}

func (h *contextLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &contextLogHandler{
		Handler:     h.Handler.WithAttrs(attrs),
		otelHandler: h.otelHandler.WithAttrs(attrs),
	}
}

func (h *contextLogHandler) WithGroup(name string) slog.Handler {
	return &contextLogHandler{
		Handler:     h.Handler.WithGroup(name),
		otelHandler: h.otelHandler.WithGroup(name),
	}
}

func recordPollCycle(ctx context.Context, status string, durationMs float64) {
	m := otel.Meter(bmkgPollerScope)
	attrs := metric.WithAttributes(attribute.String("status", status))
	if c, err := m.Int64Counter(
		"bmkg_poller.poll.cycles_total",
		metric.WithDescription("Total number of BMKG polling cycles executed"),
	); err == nil {
		c.Add(ctx, 1, attrs)
	}
	if h, err := m.Float64Histogram(
		"bmkg_poller.poll.duration_ms",
		metric.WithDescription("Duration of BMKG polling cycles in milliseconds"),
		metric.WithUnit("ms"),
	); err == nil {
		h.Record(ctx, durationMs, attrs)
	}
}

func recordHTTPClientDuration(ctx context.Context, endpoint string, statusCode int, durationMs float64) {
	if h, err := otel.Meter(bmkgPollerScope).Float64Histogram(
		"bmkg_poller.http.client_duration_ms",
		metric.WithDescription("Duration of outbound HTTP requests to BMKG in milliseconds"),
		metric.WithUnit("ms"),
	); err == nil {
		h.Record(ctx, durationMs, metric.WithAttributes(
			attribute.String("endpoint", endpoint),
			attribute.Int("status_code", statusCode),
		))
	}
}

func recordEventsFetched(ctx context.Context, eventType string, count int) {
	if c, err := otel.Meter(bmkgPollerScope).Int64Counter(
		"bmkg_poller.events.fetched_total",
		metric.WithDescription("Total number of events fetched from BMKG"),
	); err == nil {
		c.Add(ctx, int64(count), metric.WithAttributes(attribute.String("type", eventType)))
	}
}

func recordKafkaPublish(ctx context.Context, topic, severity, status string, durationMs float64) {
	m := otel.Meter(bmkgPollerScope)
	if c, err := m.Int64Counter(
		"bmkg_poller.kafka.published_total",
		metric.WithDescription("Total number of normalized hazard events published to Kafka"),
	); err == nil {
		c.Add(ctx, 1, metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("severity", severity),
			attribute.String("status", status),
		))
	}
	if h, err := m.Float64Histogram(
		"bmkg_poller.kafka.publish_duration_ms",
		metric.WithDescription("Duration of publishing hazard events to Kafka in milliseconds"),
		metric.WithUnit("ms"),
	); err == nil {
		h.Record(ctx, durationMs, metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("status", status),
		))
	}
}

func initTelemetry(ctx context.Context) (func(context.Context) error, error) {
	serviceName := envOrDefault("OTEL_SERVICE_NAME", bmkgPollerScope)

	res, err := resource.New(
		ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		return nil, err
	}

	var traceOpts []otlptracegrpc.Option
	var metricOpts []otlpmetricgrpc.Option
	var logOpts []otlploggrpc.Option
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		traceOpts = append(traceOpts, otlptracegrpc.WithInsecure())
		metricOpts = append(metricOpts, otlpmetricgrpc.WithInsecure())
		logOpts = append(logOpts, otlploggrpc.WithInsecure())
	}

	traceExporter, err := otlptracegrpc.New(ctx, traceOpts...)
	if err != nil {
		return nil, err
	}

	metricExporter, err := otlpmetricgrpc.New(ctx, metricOpts...)
	if err != nil {
		return nil, err
	}

	logExporter, err := otlploggrpc.New(ctx, logOpts...)
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

	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		sdklog.WithResource(res),
	)
	otel.SetLoggerProvider(lp)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func(shutdownCtx context.Context) error {
		return errors.Join(tp.Shutdown(shutdownCtx), mp.Shutdown(shutdownCtx), lp.Shutdown(shutdownCtx))
	}, nil
}

type kafkaHeaderCarrier []kafka.Header

func (c *kafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *kafkaHeaderCarrier) Set(key, val string) {
	for i, h := range *c {
		if h.Key == key {
			(*c)[i].Value = []byte(val)
			return
		}
	}
	*c = append(*c, kafka.Header{Key: key, Value: []byte(val)})
}

func (c *kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(*c))
	for _, h := range *c {
		keys = append(keys, h.Key)
	}
	return keys
}
