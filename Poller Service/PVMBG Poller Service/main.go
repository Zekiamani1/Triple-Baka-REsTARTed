package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"pvmbgpoll/proto"

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

type VolcanoReport struct {
	ReportID         string         `json:"report_id"`
	VolcanoID        string         `json:"volcano_id"`
	AlertLevel       string         `json:"alert_level"`
	EruptionCount24h int            `json:"eruption_count_24h" `
	AshColumnHeightM float64        `json:"ash_column_height_m" `
	ReportedAt       time.Time      `json:"reported_at"`
	Extra            map[string]any `json:",embed"`
}

type volcanoReference struct {
	areaName  string
	latitude  float64
	longitude float64
}

var volcanoReferences = map[string]volcanoReference{
	"Sumbing":          {"Gunung Sumbing", -7.384, 110.072},
	"Salak":            {"Gunung Salak", -6.719, 106.733},
	"Gede":             {"Gunung Gede", -6.770, 106.999},
	"Tangkuban Parahu": {"Gunung Tangkuban Parahu", -6.759, 107.609},
	"Guntur":           {"Gunung Guntur", -7.143, 107.840},
	"Papandayan":       {"Gunung Papandayan", -7.319, 107.730},
	"Galunggung":       {"Gunung Galunggung", -7.256, 108.078},
	"Ciremai":          {"Gunung Ciremai", -6.892, 108.408},
	"Slamet":           {"Gunung Slamet", -7.242, 109.208},
	"Dieng":            {"Kompleks Vulkanik Dieng", -7.200, 109.920},
	"Sundoro":          {"Gunung Sundoro", -7.300, 109.992},
	"Merapi":           {"Gunung Merapi", -7.540, 110.446},
	"Kelud":            {"Gunung Kelud", -7.935, 112.314},
	"Arjuno Welirang":  {"Kompleks Arjuno-Welirang", -7.725, 112.580},
	"Bromo":            {"Gunung Bromo", -7.942, 112.953},
	"Semeru":           {"Gunung Semeru", -8.108, 112.922},
	"Lamongan":         {"Gunung Lamongan", -7.979, 113.341},
	"Raung":            {"Gunung Raung", -8.125, 114.042},
	"Ijen":             {"Kompleks Ijen", -8.058, 114.242},
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

func pvmbgRequest(ctx context.Context, targetURL string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+requiredEnv("PVMBG_API_KEY"))
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
	port := envOrDefault("HEALTH_PORT", "8086")
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
		if err := http.ListenAndServe(":"+port, mux); err != nil {
			slog.Error("health server stopped", "err", err)
		}
	}()
}

func SendToAggregate(ctx context.Context, thething *proto.Hazard) error {
	topic := envOrDefault("KAFKA_TOPIC_HAZARD_INGEST", "hazard-ingest")
	ctx, span := otel.Tracer(pvmbgPollerScope).Start(ctx, "kafka.publish "+topic,
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

func poll(ctx context.Context, since time.Time) (reports []VolcanoReport, err error) {
	pvmbg := envOrDefault("PVMBG_BASE_URL", "http://pvmbg:8082")
	targeturl := fmt.Sprintf("%s/volcanic-reports?since=%s", pvmbg, url.QueryEscape(since.Format(time.RFC3339)))

	ctx, span := otel.Tracer(pvmbgPollerScope).Start(ctx, "PVMBGPoller.fetchVolcanicReports",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("http.request.method", http.MethodGet),
			attribute.String("url.full", targeturl),
		),
	)
	defer span.End()

	start := time.Now()
	resp, err := pvmbgRequest(ctx, targeturl)
	durationMs := float64(time.Since(start).Nanoseconds()) / 1e6

	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		recordHTTPClientDuration(ctx, "volcanic-reports", 0, durationMs)
		return
	}
	defer resp.Body.Close()
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	recordHTTPClientDuration(ctx, "volcanic-reports", resp.StatusCode, durationMs)

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("PVMBG returned HTTP %d", resp.StatusCode)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return nil, err
	}
	a, err := io.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("read PVMBG response: %w", err)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return nil, err
	}

	if err = json.UnmarshalRead(bytes.NewReader(a), &reports); err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return
	}
	span.SetAttributes(attribute.Int("reports.count", len(reports)))
	recordEventsFetched(ctx, len(reports))
	return
}

func normalize(ctx context.Context, data VolcanoReport) proto.Hazard {
	attributes := data.Extra
	if attributes == nil {
		attributes = make(map[string]any)
	}
	attributes["eruption_count_24h"] = data.EruptionCount24h
	attributes["ash_column_height_m"] = data.AshColumnHeightM
	attrStruct, err := structpb.NewStruct(attributes)
	if err != nil {
		slog.ErrorContext(ctx, "failed to build protobuf struct for PVMBG attributes", "err", err)
	}
	reference := volcanoReferences[data.VolcanoID]
	var severity proto.Severity
	switch data.AlertLevel {
	case "Normal":
		severity = proto.Severity_NORMAL
	case "Waspada":
		severity = proto.Severity_WASPADA
	case "Siaga":
		severity = proto.Severity_SIAGA
	case "Awas":
		severity = proto.Severity_AWAS
	}
	return proto.Hazard{

		HazardId:    uuid.NewString(),
		Source:      proto.Source_PVMBG,
		SourceRefId: data.ReportID,
		HazardType:  proto.HazardType_VOLCANIC,
		Longitude:   reference.longitude,
		Latitude:    reference.latitude,
		Severity:    severity,
		AreaName:    reference.areaName,
		OccurredAt:  timestamppb.New(data.ReportedAt),
		IngestedAt:  timestamppb.New(time.Now()),
		Attributes:  attrStruct,
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

	slog.InfoContext(ctx, "PVMBG poller started", "interval_seconds", interval.Seconds())

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cycleStart := time.Now()
			cycleCtx, cycleSpan := otel.Tracer(pvmbgPollerScope).Start(ctx, "PVMBGPoller.PollCycle",
				trace.WithAttributes(attribute.String("poll.cursor", now.Format(time.RFC3339))),
			)

			events, err := poll(cycleCtx, now)
			if err != nil {
				slog.ErrorContext(cycleCtx, "PVMBG poll failed; keeping cursor", "cursor", now.Format(time.RFC3339), "err", err)
				cycleSpan.RecordError(err)
				cycleSpan.SetStatus(otelcodes.Error, err.Error())
				cycleSpan.End()
				durationMs := float64(time.Since(cycleStart).Nanoseconds()) / 1e6
				recordPollCycle(cycleCtx, "error", durationMs)
				continue
			}

			published := true
			for _, data := range events {
				normalized := normalize(cycleCtx, data)
				if err := SendToAggregate(cycleCtx, &normalized); err != nil {
					slog.ErrorContext(cycleCtx, "PVMBG publish failed; keeping cursor", "cursor", now.Format(time.RFC3339), "err", err)
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
