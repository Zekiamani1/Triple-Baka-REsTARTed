package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otlpmetricgrpc "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	otlptracegrpc "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Telemetry owns the OpenTelemetry providers and the small set of instruments
// used by the Sensor Pantai adapter. Metric labels are intentionally low-cardinality.
type Telemetry struct {
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	tracer         trace.Tracer

	upstreamRequests metric.Int64Counter
	upstreamDuration metric.Float64Histogram
	observations     metric.Int64Counter
	quarantined      metric.Int64Counter
	hazardsPublished metric.Int64Counter
	checkpointWrites metric.Int64Counter
	dataAge          metric.Float64Histogram

	circuitState metric.Int64ObservableGauge
	lastDataAge  metric.Float64ObservableGauge
	registration metric.Registration

	circuitSourceMu sync.RWMutex
	circuitSource   func() (CircuitState, int, time.Time)
	lastDataUnixNS  atomic.Int64
}

// NewTelemetry configures OTLP over gRPC. An http:// endpoint uses plaintext
// gRPC (for example, the in-cluster LGTM endpoint at :4317); https:// uses TLS.
func NewTelemetry(ctx context.Context, cfg Config) (*Telemetry, error) {
	endpoint, insecure, err := parseOTLPGRPCEndpoint(cfg.OTelExporterOTLPEndpoint)
	if err != nil {
		return nil, err
	}

	traceOptions := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(endpoint)}
	metricOptions := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(endpoint)}
	if insecure {
		traceOptions = append(traceOptions, otlptracegrpc.WithInsecure())
		metricOptions = append(metricOptions, otlpmetricgrpc.WithInsecure())
	}

	traceExporter, err := otlptracegrpc.New(ctx, traceOptions...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	metricExporter, err := otlpmetricgrpc.New(ctx, metricOptions...)
	if err != nil {
		_ = traceExporter.Shutdown(context.Background())
		return nil, fmt.Errorf("create OTLP metric exporter: %w", err)
	}

	res, err := sdkresource.New(ctx, sdkresource.WithAttributes(
		attribute.String("service.name", cfg.OTelServiceName),
		attribute.String("service.namespace", "sistem-informasi-kebencanaan-bnpb"),
	))
	if err != nil {
		_ = traceExporter.Shutdown(context.Background())
		_ = metricExporter.Shutdown(context.Background())
		return nil, fmt.Errorf("create telemetry resource: %w", err)
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
		sdktrace.WithBatcher(traceExporter),
	)
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(15*time.Second))),
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	meter := meterProvider.Meter(cfg.OTelServiceName)
	telemetry := &Telemetry{
		tracerProvider: tracerProvider,
		meterProvider:  meterProvider,
		tracer:         tracerProvider.Tracer(cfg.OTelServiceName),
	}

	if telemetry.upstreamRequests, err = meter.Int64Counter(
		"pantai_upstream_requests_total",
		metric.WithDescription("Number of requests made to the Sensor Pantai API."),
	); err != nil {
		return telemetry.initError(err)
	}
	if telemetry.upstreamDuration, err = meter.Float64Histogram(
		"pantai_upstream_request_duration_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of Sensor Pantai HTTP requests."),
	); err != nil {
		return telemetry.initError(err)
	}
	if telemetry.observations, err = meter.Int64Counter(
		"pantai_observations_processed_total",
		metric.WithDescription("Number of observations handled by the adapter."),
	); err != nil {
		return telemetry.initError(err)
	}
	if telemetry.quarantined, err = meter.Int64Counter(
		"pantai_quarantine_total",
		metric.WithDescription("Number of observations durably sent to quarantine."),
	); err != nil {
		return telemetry.initError(err)
	}
	if telemetry.hazardsPublished, err = meter.Int64Counter(
		"pantai_hazards_published_total",
		metric.WithDescription("Number of normalized coastal HazardEvents acknowledged by Kafka."),
	); err != nil {
		return telemetry.initError(err)
	}
	if telemetry.checkpointWrites, err = meter.Int64Counter(
		"pantai_checkpoint_writes_total",
		metric.WithDescription("Number of successful durable cursor checkpoint writes."),
	); err != nil {
		return telemetry.initError(err)
	}
	if telemetry.dataAge, err = meter.Float64Histogram(
		"pantai_observation_age_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Age of observations when processed by the adapter."),
	); err != nil {
		return telemetry.initError(err)
	}
	if telemetry.circuitState, err = meter.Int64ObservableGauge(
		"pantai_circuit_state",
		metric.WithDescription("Circuit breaker state: 0=closed, 1=open, 2=half_open."),
	); err != nil {
		return telemetry.initError(err)
	}
	if telemetry.lastDataAge, err = meter.Float64ObservableGauge(
		"pantai_last_data_age_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Age of the last successfully processed observation."),
	); err != nil {
		return telemetry.initError(err)
	}

	registration, err := meter.RegisterCallback(telemetry.observeState, telemetry.circuitState, telemetry.lastDataAge)
	if err != nil {
		return telemetry.initError(err)
	}
	telemetry.registration = registration
	return telemetry, nil
}

func (t *Telemetry) initError(cause error) (*Telemetry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if t != nil {
		_ = t.Shutdown(ctx)
	}
	return nil, fmt.Errorf("create OpenTelemetry instruments: %w", cause)
}

func parseOTLPGRPCEndpoint(raw string) (endpoint string, insecure bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT must not be empty")
	}

	parsed, parseErr := url.Parse(raw)
	if parseErr != nil {
		return "", false, fmt.Errorf("parse OTEL_EXPORTER_OTLP_ENDPOINT: %w", parseErr)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", false, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT must use http or https")
	}
	if parsed.Host == "" {
		return "", false, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT must include a host and port")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", false, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT must be a gRPC host URL without a path")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", false, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT must not contain a query or fragment")
	}
	return parsed.Host, parsed.Scheme == "http", nil
}

func (t *Telemetry) observeState(_ context.Context, observer metric.Observer) error {
	if t == nil {
		return nil
	}

	t.circuitSourceMu.RLock()
	source := t.circuitSource
	t.circuitSourceMu.RUnlock()
	if source != nil {
		state, _, _ := source()
		observer.ObserveInt64(
			t.circuitState,
			int64(state),
			metric.WithAttributes(attribute.String("state", state.String())),
		)
	}

	lastData := t.lastDataUnixNS.Load()
	if lastData > 0 {
		age := time.Since(time.Unix(0, lastData))
		if age < 0 {
			age = 0
		}
		observer.ObserveFloat64(t.lastDataAge, age.Seconds())
	}
	return nil
}

// SetCircuitBreakerSource connects the observable gauge to the SensorClient's
// thread-safe breaker snapshot.
func (t *Telemetry) SetCircuitBreakerSource(source func() (CircuitState, int, time.Time)) {
	if t == nil {
		return
	}
	t.circuitSourceMu.Lock()
	t.circuitSource = source
	t.circuitSourceMu.Unlock()
}

func (t *Telemetry) StartSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	if t == nil || t.tracer == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	return t.tracer.Start(ctx, name)
}

// CorrelationID returns the trace ID when a span exists, or a random ID as a
// fallback. The value is suitable for the correlation-id Kafka header.
func (t *Telemetry) CorrelationID(ctx context.Context) string {
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		return spanContext.TraceID().String()
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err == nil {
		return hex.EncodeToString(id[:])
	}
	return fmt.Sprintf("fallback-%d", time.Now().UTC().UnixNano())
}

func (t *Telemetry) RecordUpstream(ctx context.Context, endpoint, outcome string, startedAt time.Time) {
	if t == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("endpoint", endpoint),
		attribute.String("outcome", outcome),
	)
	t.upstreamRequests.Add(ctx, 1, attrs)
	if !startedAt.IsZero() {
		duration := time.Since(startedAt).Seconds()
		if duration < 0 {
			duration = 0
		}
		t.upstreamDuration.Record(ctx, duration, attrs)
	}
}

func (t *Telemetry) RecordObservation(ctx context.Context, outcome string) {
	if t != nil {
		t.observations.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	}
}

func (t *Telemetry) RecordQuarantine(ctx context.Context, reason ValidationReason) {
	if t == nil {
		return
	}
	t.quarantined.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", string(reason))))
	t.RecordObservation(ctx, "quarantined")
}

func (t *Telemetry) RecordHazardPublished(ctx context.Context) {
	if t == nil {
		return
	}
	t.hazardsPublished.Add(ctx, 1)
	t.RecordObservation(ctx, "published")
}

// SetLastDataAt seeds the last-data-age gauge from the durable source status
// loaded during startup. It does not emit a histogram observation.
func (t *Telemetry) SetLastDataAt(observedAt time.Time) {
	if t == nil || observedAt.IsZero() {
		return
	}
	t.lastDataUnixNS.Store(observedAt.UTC().UnixNano())
}

func (t *Telemetry) RecordDataAge(ctx context.Context, observedAt time.Time) {
	if t == nil || observedAt.IsZero() {
		return
	}
	age := time.Since(observedAt)
	if age < 0 {
		age = 0
	}
	t.dataAge.Record(ctx, age.Seconds())
	t.lastDataUnixNS.Store(observedAt.UTC().UnixNano())
}

func (t *Telemetry) RecordCheckpoint(ctx context.Context) {
	if t != nil {
		t.checkpointWrites.Add(ctx, 1)
	}
}

// Shutdown flushes telemetry with a bounded caller-provided context.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	var firstErr error
	if t.registration != nil {
		if err := t.registration.Unregister(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("unregister telemetry callback: %w", err)
		}
	}
	if t.meterProvider != nil {
		if err := t.meterProvider.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("shutdown meter provider: %w", err)
		}
	}
	if t.tracerProvider != nil {
		if err := t.tracerProvider.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("shutdown tracer provider: %w", err)
		}
	}
	return firstErr
}
