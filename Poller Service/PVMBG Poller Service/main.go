package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"pvmbgpoll/proto"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
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
		log.Fatalf("%s must be configured", name)
	}
	return value
}

func pvmbgRequest(targetURL string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+requiredEnv("PVMBG_API_KEY"))
	return httpClient().Do(req)
}

func httpClient() *http.Client {
	seconds, err := strconv.Atoi(envOrDefault("HTTP_TIMEOUT_SECONDS", "5"))
	if err != nil || seconds <= 0 {
		log.Fatalf("HTTP_TIMEOUT_SECONDS must be a positive integer, got %q", os.Getenv("HTTP_TIMEOUT_SECONDS"))
	}
	return &http.Client{Timeout: time.Duration(seconds) * time.Second}
}

func startHealthServer() {
	port := envOrDefault("HEALTH_PORT", "8086")
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
		if err := http.ListenAndServe(":"+port, mux); err != nil {
			log.Printf("health server stopped: %v", err)
		}
	}()
}

func SendToAggregate(thething *proto.Hazard) error {
	payload, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(thething)
	if err != nil {
		return fmt.Errorf("encode HazardEvent: %w", err)
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(envOrDefault("KAFKA_BROKERS", "kafka-aggregate:29092")),
		Topic:    envOrDefault("KAFKA_TOPIC_HAZARD_INGEST", "hazard-ingest"),
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(thething.HazardId),
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("publish HazardEvent to Kafka: %w", err)
	}
	return nil
}

type fetchresult[T any] struct {
	err    error
	result T
}

func poll(since time.Time) (reports []VolcanoReport, err error) {
	pvmbg := envOrDefault("PVMBG_BASE_URL", "http://pvmbg:8082")

	targeturl := fmt.Sprintf("%s/volcanic-reports?since=%s", pvmbg, url.QueryEscape(since.Format(time.RFC3339)))
	resp, err := pvmbgRequest(targeturl)

	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("PVMBG returned HTTP %d", resp.StatusCode)
	}
	a, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read PVMBG response: %w", err)
	}

	if err = json.UnmarshalRead(bytes.NewReader(a), &reports); err != nil {
		return
	}
	return
}

func normalize(data VolcanoReport) proto.Hazard {
	attributes := data.Extra
	if attributes == nil {
		attributes = make(map[string]any)
	}
	attributes["eruption_count_24h"] = data.EruptionCount24h
	attributes["ash_column_height_m"] = data.AshColumnHeightM
	attribute, err := structpb.NewStruct(attributes)
	if err != nil {
		println(err.Error())
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
		Attributes:  attribute,
	}
}
func main() {
	startHealthServer()
	interval := pollInterval()
	now := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		events, err := poll(now)
		if err != nil {
			log.Printf("PVMBG poll failed; keeping cursor %s: %v", now.Format(time.RFC3339), err)
			continue
		}
		published := true
		for _, data := range events {
			normalized := normalize(data)
			if err := SendToAggregate(&normalized); err != nil {
				log.Printf("PVMBG publish failed; keeping cursor %s: %v", now.Format(time.RFC3339), err)
				published = false
				break
			}
		}
		if published {
			now = time.Now()
		}
	}
}

func pollInterval() time.Duration {
	seconds := 3
	if value := os.Getenv("POLL_INTERVAL_SECONDS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			log.Fatalf("POLL_INTERVAL_SECONDS must be a positive integer, got %q", value)
		}
		seconds = parsed
	}
	return time.Duration(seconds) * time.Second
}
