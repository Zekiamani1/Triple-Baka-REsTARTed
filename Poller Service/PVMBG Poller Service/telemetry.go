package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
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

const pvmbgPollerScope = "pvmbg-poller"

var (
	pollCyclesCounter        metric.Int64Counter
	pollDurationHist         metric.Float64Histogram
	httpClientDurationHist   metric.Float64Histogram
	eventsFetchedCounter     metric.Int64Counter
	kafkaPublishedCounter    metric.Int64Counter
	kafkaPublishDurationHist metric.Float64Histogram
)

func initInstruments(m metric.Meter) {
	if c, err := m.Int64Counter(
		"pvmbg_poller.poll.cycles_total",
		metric.WithDescription("Total number of PVMBG polling cycles executed"),
	); err == nil {
		pollCyclesCounter = c
	}
	if h, err := m.Float64Histogram(
		"pvmbg_poller.poll.duration_ms",
		metric.WithDescription("Duration of PVMBG polling cycles in milliseconds"),
		metric.WithUnit("ms"),
	); err == nil {
		pollDurationHist = h
	}
	if h, err := m.Float64Histogram(
		"pvmbg_poller.http.client_duration_ms",
		metric.WithDescription("Duration of outbound HTTP requests to PVMBG in milliseconds"),
		metric.WithUnit("ms"),
	); err == nil {
		httpClientDurationHist = h
	}
	if c, err := m.Int64Counter(
		"pvmbg_poller.events.fetched_total",
		metric.WithDescription("Total number of volcanic reports fetched from PVMBG"),
	); err == nil {
		eventsFetchedCounter = c
	}
	if c, err := m.Int64Counter(
		"pvmbg_poller.kafka.published_total",
		metric.WithDescription("Total number of normalized hazard events published to Kafka"),
	); err == nil {
		kafkaPublishedCounter = c
	}
	if h, err := m.Float64Histogram(
		"pvmbg_poller.kafka.publish_duration_ms",
		metric.WithDescription("Duration of publishing hazard events to Kafka in milliseconds"),
		metric.WithUnit("ms"),
	); err == nil {
		kafkaPublishDurationHist = h
	}
}

func samplerFromEnv() sdktrace.Sampler {
	samplerName := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER")))
	ratio := 0.05
	if rawArg := strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER_ARG")); rawArg != "" {
		if parsed, err := strconv.ParseFloat(rawArg, 64); err == nil && parsed >= 0 && parsed <= 1 {
			ratio = parsed
		}
	}

	switch samplerName {
	case "always_on":
		return sdktrace.AlwaysSample()
	case "always_off":
		return sdktrace.NeverSample()
	case "traceidratio":
		return sdktrace.TraceIDRatioBased(ratio)
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample())
	case "parentbased_traceidratio":
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	case "parentbased_always_on", "":
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	default:
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	}
}

type contextLogHandler struct {
	slog.Handler
	otelHandler slog.Handler
}

func newContextLogHandler(h slog.Handler) *contextLogHandler {
	return &contextLogHandler{
		Handler:     h,
		otelHandler: otelslog.NewHandler(pvmbgPollerScope),
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
	attrs := metric.WithAttributes(attribute.String("status", status))
	if pollCyclesCounter != nil {
		pollCyclesCounter.Add(ctx, 1, attrs)
	}
	if pollDurationHist != nil {
		pollDurationHist.Record(ctx, durationMs, attrs)
	}
}

func recordHTTPClientDuration(ctx context.Context, endpoint string, statusCode int, durationMs float64) {
	if httpClientDurationHist != nil {
		httpClientDurationHist.Record(ctx, durationMs, metric.WithAttributes(
			attribute.String("endpoint", endpoint),
			attribute.Int("status_code", statusCode),
		))
	}
}

func recordEventsFetched(ctx context.Context, count int) {
	if eventsFetchedCounter != nil {
		eventsFetchedCounter.Add(ctx, int64(count))
	}
}

func recordKafkaPublish(ctx context.Context, topic, severity, status string, durationMs float64) {
	if kafkaPublishedCounter != nil {
		kafkaPublishedCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("severity", severity),
			attribute.String("status", status),
		))
	}
	if kafkaPublishDurationHist != nil {
		kafkaPublishDurationHist.Record(ctx, durationMs, metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("status", status),
		))
	}
}

func initTelemetry(ctx context.Context) (func(context.Context) error, error) {
	serviceName := envOrDefault("OTEL_SERVICE_NAME", pvmbgPollerScope)

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
		sdktrace.WithSampler(samplerFromEnv()),
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(10*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	initInstruments(otel.Meter(pvmbgPollerScope))

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
