package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"bmkg-poller/hazard"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SeismicEvent struct {
	EventID          string    `json:"event_id"`
	Magnitude        float64   `json:"magnitude"`
	DepthKM          float64   `json:"depth_km"`
	EpicenterLat     float64   `json:"epicenter_lat"`
	EpicenterLon     float64   `json:"epicenter_lon"`
	RegionName       string    `json:"region_name"`
	OccurredAt       time.Time `json:"occurred_at"`
	PotentialTsunami bool      `json:"potential_tsunami"`
}
type TsunamiWarning struct {
	WarningID        string    `json:"warning_id"`
	RelatedEventID   string    `json:"related_event_id"`
	ThreatLevel      string    `json:"threat_level"`
	AffectedZones    string    `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func requiredEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		slog.Error("required environment variable not configured", "env", name)
		os.Exit(1)
	}
	return value
}

func bmkgRequest(ctx context.Context, targetURL string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-BMKG-Key", requiredEnv("BMKG_API_KEY"))
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	return httpClient().Do(req)
}

func httpClient() *http.Client {
	seconds, err := strconv.Atoi(envOrDefault("HTTP_TIMEOUT_SECONDS", "5"))
	if err != nil || seconds <= 0 {
		slog.Error("HTTP_TIMEOUT_SECONDS must be a positive integer", "value", os.Getenv("HTTP_TIMEOUT_SECONDS"))
		os.Exit(1)
	}
	return &http.Client{Timeout: time.Duration(seconds) * time.Second}
}

func startHealthServer() {
	port := envOrDefault("HEALTH_PORT", "8085")
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
		if err := http.ListenAndServe(":"+port, mux); err != nil {
			slog.Error("health server stopped", "err", err)
		}
	}()
}

func SendToAggregate(ctx context.Context, thething *hazard.Hazard) error {
	topic := envOrDefault("KAFKA_TOPIC_HAZARD_INGEST", "hazard-ingest")
	ctx, span := otel.Tracer(bmkgPollerScope).Start(ctx, "kafka.publish "+topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation", "publish"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("hazard.id", thething.HazardId),
			attribute.String("hazard.source", thething.Source.String()),
			attribute.String("hazard.type", thething.HazardType.String()),
			attribute.String("hazard.severity", thething.Severity.String()),
		),
	)
	defer span.End()

	start := time.Now()
	payload, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(thething)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return fmt.Errorf("encode HazardEvent: %w", err)
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(envOrDefault("KAFKA_BROKERS", "kafka-aggregate:29092")),
		Topic:    topic,
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()

	var headers kafkaHeaderCarrier
	otel.GetTextMapPropagator().Inject(ctx, &headers)

	err = writer.WriteMessages(ctx, kafka.Message{
		Key:     []byte(thething.HazardId),
		Value:   payload,
		Headers: headers,
	})
	durationMs := float64(time.Since(start).Nanoseconds()) / 1e6
	statusStr := "success"
	if err != nil {
		statusStr = "error"
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
	} else {
		span.SetStatus(otelcodes.Ok, "")
	}

	recordKafkaPublish(ctx, topic, thething.Severity.String(), statusStr, durationMs)

	if err != nil {
		return fmt.Errorf("publish HazardEvent to Kafka: %w", err)
	}
	return nil
}

type fetchresult[T any] struct {
	err    error
	result T
}

func fetchSeismicEvent(ctx context.Context, since time.Time, channel chan fetchresult[[]SeismicEvent]) {
	var events []SeismicEvent
	bmkgadress := envOrDefault("BMKG_BASE_URL", "http://bmkg:8081")
	targeturl := fmt.Sprintf("%s/seismic-events?since=%s", bmkgadress, url.QueryEscape(since.Format(time.RFC3339)))

	ctx, span := otel.Tracer(bmkgPollerScope).Start(ctx, "BMKGPoller.fetchSeismicEvent",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("http.request.method", http.MethodGet),
			attribute.String("url.full", targeturl),
		),
	)
	defer span.End()

	start := time.Now()
	resp, err := bmkgRequest(ctx, targeturl)
	durationMs := float64(time.Since(start).Nanoseconds()) / 1e6

	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		recordHTTPClientDuration(ctx, "seismic-events", 0, durationMs)
		slog.ErrorContext(ctx, "fetch seismic events request failed", "err", err)
		channel <- fetchresult[[]SeismicEvent]{err: err, result: nil}
		return
	}
	defer resp.Body.Close()
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	recordHTTPClientDuration(ctx, "seismic-events", resp.StatusCode, durationMs)

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("BMKG returned HTTP %d", resp.StatusCode)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		channel <- fetchresult[[]SeismicEvent]{err: err, result: nil}
		return
	}

	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		slog.ErrorContext(ctx, "decode seismic events JSON failed", "err", err)
		channel <- fetchresult[[]SeismicEvent]{err: err, result: nil}
		return
	}
	span.SetAttributes(attribute.Int("events.count", len(events)))
	recordEventsFetched(ctx, "seismic", len(events))
	channel <- fetchresult[[]SeismicEvent]{err: nil, result: events}
}

func fetchTsunamiWarning(ctx context.Context, since time.Time, channel chan fetchresult[[]TsunamiWarning]) {
	var warnings []TsunamiWarning
	bmkgadress := envOrDefault("BMKG_BASE_URL", "http://bmkg:8081")
	targeturl := fmt.Sprintf("%s/tsunami-warnings?since=%s", bmkgadress, url.QueryEscape(since.Format(time.RFC3339)))

	ctx, span := otel.Tracer(bmkgPollerScope).Start(ctx, "BMKGPoller.fetchTsunamiWarning",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("http.request.method", http.MethodGet),
			attribute.String("url.full", targeturl),
		),
	)
	defer span.End()

	start := time.Now()
	resp, err := bmkgRequest(ctx, targeturl)
	durationMs := float64(time.Since(start).Nanoseconds()) / 1e6

	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		recordHTTPClientDuration(ctx, "tsunami-warnings", 0, durationMs)
		slog.ErrorContext(ctx, "fetch tsunami warnings request failed", "err", err)
		channel <- fetchresult[[]TsunamiWarning]{err: err, result: nil}
		return
	}
	defer resp.Body.Close()
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	recordHTTPClientDuration(ctx, "tsunami-warnings", resp.StatusCode, durationMs)

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("BMKG returned HTTP %d", resp.StatusCode)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		channel <- fetchresult[[]TsunamiWarning]{err: err, result: nil}
		return
	}
	if err := json.NewDecoder(resp.Body).Decode(&warnings); err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		slog.ErrorContext(ctx, "decode tsunami warnings JSON failed", "err", err)
		channel <- fetchresult[[]TsunamiWarning]{err: err, result: nil}
		return
	}
	span.SetAttributes(attribute.Int("warnings.count", len(warnings)))
	recordEventsFetched(ctx, "tsunami", len(warnings))
	channel <- fetchresult[[]TsunamiWarning]{err: nil, result: warnings}
}

func poll(ctx context.Context, since time.Time) ([]SeismicEvent, []TsunamiWarning, error) {
	chan1 := make(chan fetchresult[[]SeismicEvent])
	chan2 := make(chan fetchresult[[]TsunamiWarning])
	go fetchSeismicEvent(ctx, since, chan1)
	go fetchTsunamiWarning(ctx, since, chan2)
	result1 := <-chan1
	if result1.err != nil {
		return nil, nil, result1.err
	}
	result2 := <-chan2
	if result2.err != nil {
		return nil, nil, result2.err
	}
	return result1.result, result2.result, nil
}
func normalize(ctx context.Context, data SeismicEvent, warnings []TsunamiWarning) hazard.Hazard {
	attributemap := map[string]any{
		"magnitude":         data.Magnitude,
		"depth_km":          data.DepthKM,
		"potential_tsunami": data.PotentialTsunami,
	}
	if data.PotentialTsunami {
		for _, warn := range warnings {
			if warn.RelatedEventID == data.EventID {
				attributemap["warning_id"] = warn.WarningID
				attributemap["affected_zones"] = warn.AffectedZones
				attributemap["threat_level"] = warn.ThreatLevel
				attributemap["estimated_arrival"] = warn.EstimatedArrival.Format(time.RFC3339)
				break
			}
		}
	}

	attrStruct, err := structpb.NewStruct(attributemap)
	if err != nil {
		slog.ErrorContext(ctx, "failed to build protobuf struct for BMKG attributes", "err", err)
	}
	var severity hazard.Severity

	switch attributemap["threat_level"] {
	case "Waspada":
		severity = hazard.Severity_WASPADA
	case "Siaga":
		severity = hazard.Severity_SIAGA
	case "Awas":
		severity = hazard.Severity_AWAS
	default:
		switch {
		case data.Magnitude < 5.0:
			severity = hazard.Severity_NORMAL
		case 5.0 <= data.Magnitude && data.Magnitude < 6.5:
			severity = hazard.Severity_WASPADA
		case 6.5 <= data.Magnitude:
			severity = hazard.Severity_SIAGA

		}
	}
	return hazard.Hazard{

		HazardId:    uuid.NewString(),
		Source:      hazard.Source_BMKG,
		SourceRefId: data.EventID,
		HazardType:  hazard.HazardType_SEISMIC,

		Severity: severity,

		AreaName:   data.RegionName,
		Latitude:   data.EpicenterLat,
		Longitude:  data.EpicenterLon,
		OccurredAt: timestamppb.New(data.OccurredAt),
		IngestedAt: timestamppb.New(time.Now()),
		Attributes: attrStruct,
	}
}
func main() {
	slog.SetDefault(slog.New(newContextLogHandler(slog.NewJSONHandler(os.Stdout, nil))))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTelemetry, err := initTelemetry(ctx)
	if err != nil {
		slog.WarnContext(ctx, "failed to initialize OpenTelemetry", "err", err)
	} else {
		defer func() {
			otelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdownTelemetry(otelCtx)
		}()
	}

	startHealthServer()
	interval := pollInterval()
	now := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.InfoContext(ctx, "BMKG poller started", "interval_seconds", interval.Seconds())

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cycleStart := time.Now()
			cycleCtx, cycleSpan := otel.Tracer(bmkgPollerScope).Start(ctx, "BMKGPoller.PollCycle",
				trace.WithAttributes(attribute.String("poll.cursor", now.Format(time.RFC3339))),
			)

			events, warnings, err := poll(cycleCtx, now)
			if err != nil {
				slog.ErrorContext(cycleCtx, "BMKG poll failed; keeping cursor", "cursor", now.Format(time.RFC3339), "err", err)
				cycleSpan.RecordError(err)
				cycleSpan.SetStatus(otelcodes.Error, err.Error())
				cycleSpan.End()
				durationMs := float64(time.Since(cycleStart).Nanoseconds()) / 1e6
				recordPollCycle(cycleCtx, "error", durationMs)
				continue
			}

			published := true
			for _, data := range events {
				normalized := normalize(cycleCtx, data, warnings)
				if err := SendToAggregate(cycleCtx, &normalized); err != nil {
					slog.ErrorContext(cycleCtx, "BMKG publish failed; keeping cursor", "cursor", now.Format(time.RFC3339), "err", err)
					cycleSpan.RecordError(err)
					cycleSpan.SetStatus(otelcodes.Error, err.Error())
					published = false
					break
				}
			}

			durationMs := float64(time.Since(cycleStart).Nanoseconds()) / 1e6
			if published {
				now = time.Now()
				cycleSpan.SetStatus(otelcodes.Ok, "")
				recordPollCycle(cycleCtx, "success", durationMs)
			} else {
				recordPollCycle(cycleCtx, "error", durationMs)
			}
			cycleSpan.End()
		}
	}
}

func pollInterval() time.Duration {
	seconds := 3
	if value := os.Getenv("POLL_INTERVAL_SECONDS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			slog.Error("POLL_INTERVAL_SECONDS must be a positive integer", "value", value)
			os.Exit(1)
		}
		seconds = parsed
	}
	return time.Duration(seconds) * time.Second
}
