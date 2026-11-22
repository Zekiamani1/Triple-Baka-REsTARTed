package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"os"
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
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const Scope = "satellite-poller"

// RuntimeSnapshot supplies instantaneous queue/cluster gauges to OTel callbacks.
type RuntimeSnapshot struct {
	ActiveClusters  int64
	SingleHotspots  int64
	OutboxPending   int64
	IngestLag       int64
	ProcessingLag   int64
}

type contextLogHandler struct {
	slog.Handler
	otelHandler slog.Handler
}

// NewContextLogger builds a structured JSON stdout logger with trace/span ID
// correlation and forwards logs to the OpenTelemetry log bridge.
func NewContextLogger(level slog.Level) *slog.Logger {
	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(&contextLogHandler{
		Handler:     base,
		otelHandler: otelslog.NewHandler(Scope),
	})
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

// Init configures OpenTelemetry traces, metrics, logs, and W3C propagation.
func Init(ctx context.Context) (func(context.Context) error, error) {
	serviceName := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME"))
	if serviceName == "" {
		serviceName = Scope
	}

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

	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if endpoint == "" || !strings.HasPrefix(strings.ToLower(endpoint), "https://") {
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

// RecordEventIngested increments ingested SSE event counters labeled by event type.
func RecordEventIngested(ctx context.Context, eventType string) {
	if c, err := otel.Meter(Scope).Int64Counter(
		"satellite_poller.events.ingested_total",
		metric.WithDescription("Total raw satellite SSE events persisted to SQLite inbox"),
	); err == nil {
		c.Add(ctx, 1, metric.WithAttributes(attribute.String("type", eventType)))
	}
}

// RecordBatchProcessed increments inbox processing counters for events, duplicates, filtered, and late detections.
func RecordBatchProcessed(ctx context.Context, events, duplicates, filtered, late int) {
	m := otel.Meter(Scope)
	if events > 0 {
		if c, err := m.Int64Counter(
			"satellite_poller.events.processed_total",
			metric.WithDescription("Total inbox events processed by satellite processor"),
		); err == nil {
			c.Add(ctx, int64(events))
		}
	}
	if duplicates > 0 {
		if c, err := m.Int64Counter(
			"satellite_poller.events.duplicates_total",
			metric.WithDescription("Total duplicate detections/retracts skipped by satellite processor"),
		); err == nil {
			c.Add(ctx, int64(duplicates))
		}
	}
	if filtered > 0 {
		if c, err := m.Int64Counter(
			"satellite_poller.detections.filtered_total",
			metric.WithDescription("Total low-confidence hotspot detections filtered"),
		); err == nil {
			c.Add(ctx, int64(filtered))
		}
	}
	if late > 0 {
		if c, err := m.Int64Counter(
			"satellite_poller.detections.late_total",
			metric.WithDescription("Total late-arriving hotspot detections excluded from active clustering"),
		); err == nil {
			c.Add(ctx, int64(late))
		}
	}
}

// RecordKafkaPublish records Kafka outbox publish outcomes and durations.
func RecordKafkaPublish(ctx context.Context, topic, operation, status string, durationMs float64) {
	m := otel.Meter(Scope)
	attrs := metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("operation", operation),
		attribute.String("status", status),
	)
	if c, err := m.Int64Counter(
		"satellite_poller.kafka.published_total",
		metric.WithDescription("Total satellite hazard messages published to Kafka"),
	); err == nil {
		c.Add(ctx, 1, attrs)
	}
	if h, err := m.Float64Histogram(
		"satellite_poller.kafka.publish_duration_ms",
		metric.WithDescription("Duration of publishing satellite outbox batches to Kafka in milliseconds"),
		metric.WithUnit("ms"),
	); err == nil {
		h.Record(ctx, durationMs, metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("status", status),
		))
	}
}

// RegisterRuntimeMetrics attaches observable gauges for clusters, singletons, outbox backlog, and lag.
func RegisterRuntimeMetrics(snapshotFn func(context.Context) RuntimeSnapshot) (metric.Registration, error) {
	if snapshotFn == nil {
		return nil, nil
	}
	m := otel.Meter(Scope)
	activeClusters, err := m.Int64ObservableGauge(
		"satellite_poller.clusters.active",
		metric.WithDescription("Current number of active wildfire hotspot clusters"),
	)
	if err != nil {
		return nil, err
	}
	singleDetections, err := m.Int64ObservableGauge(
		"satellite_poller.detections.single",
		metric.WithDescription("Current number of unclustered single active hotspot detections"),
	)
	if err != nil {
		return nil, err
	}
	outboxPending, err := m.Int64ObservableGauge(
		"satellite_poller.outbox.pending",
		metric.WithDescription("Current pending satellite outbox records waiting for Kafka acknowledgment"),
	)
	if err != nil {
		return nil, err
	}
	ingestLag, err := m.Int64ObservableGauge(
		"satellite_poller.feed.lag_offsets",
		metric.WithDescription("Estimated offset lag between satellite session latest_offset and durable feed checkpoint"),
	)
	if err != nil {
		return nil, err
	}
	processingLag, err := m.Int64ObservableGauge(
		"satellite_poller.processing.lag_offsets",
		metric.WithDescription("Offset lag between durable feed checkpoint and clustering processor checkpoint"),
	)
	if err != nil {
		return nil, err
	}

	return m.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		snap := snapshotFn(ctx)
		observer.ObserveInt64(activeClusters, snap.ActiveClusters)
		observer.ObserveInt64(singleDetections, snap.SingleHotspots)
		observer.ObserveInt64(outboxPending, snap.OutboxPending)
		observer.ObserveInt64(ingestLag, snap.IngestLag)
		observer.ObserveInt64(processingLag, snap.ProcessingLag)
		return nil
	}, activeClusters, singleDetections, outboxPending, ingestLag, processingLag)
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

// InjectKafkaHeaders writes W3C TraceContext/Baggage headers from ctx into Kafka message headers.
func InjectKafkaHeaders(ctx context.Context, headers []kafka.Header) []kafka.Header {
	carrier := kafkaHeaderCarrier(headers)
	otel.GetTextMapPropagator().Inject(ctx, &carrier)
	return []kafka.Header(carrier)
}
