package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"bmkg-poller/hazard"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
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
		log.Fatalf("%s must be configured", name)
	}
	return value
}

func bmkgRequest(targetURL string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-BMKG-Key", requiredEnv("BMKG_API_KEY"))
	return http.DefaultClient.Do(req)
}

func SendToAggregate(thething *hazard.Hazard) {
	payload, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(thething)
	if err != nil {
		log.Printf("Failed to encode HazardEvent: %v", err)
		return
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
		log.Printf("Failed to publish HazardEvent to Kafka: %v", err)
	}
}

type fetchresult[T any] struct {
	err    error
	result T
}

func fetchSeismicEvent(since time.Time, channel chan fetchresult[[]SeismicEvent]) {
	var events []SeismicEvent
	bmkgadress := envOrDefault("BMKG_BASE_URL", "http://bmkg:8081")

	targeturl := fmt.Sprintf("%s/seismic-events?since=%s", bmkgadress, url.QueryEscape(since.Format(time.RFC3339)))
	resp, err := bmkgRequest(targeturl)

	if err != nil {
		log.Print(err.Error())
		channel <- fetchresult[[]SeismicEvent]{err: err, result: nil}
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("BMKG returned HTTP %d", resp.StatusCode)
		channel <- fetchresult[[]SeismicEvent]{err: err, result: nil}
		return
	}

	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		log.Printf("Error decoding JSON: %v", err)
		channel <- fetchresult[[]SeismicEvent]{err: err, result: nil}
		return
	}
	channel <- fetchresult[[]SeismicEvent]{err: nil, result: events}
}
func fetchTsunamiWarning(since time.Time, channel chan fetchresult[[]TsunamiWarning]) {
	var warnings []TsunamiWarning
	bmkgadress := envOrDefault("BMKG_BASE_URL", "http://bmkg:8081")

	targeturl := fmt.Sprintf("%s/tsunami-warnings?since=%s", bmkgadress, url.QueryEscape(since.Format(time.RFC3339)))
	resp, err := bmkgRequest(targeturl)
	if err != nil {
		log.Print(err.Error())
		channel <- fetchresult[[]TsunamiWarning]{err: err, result: nil}
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("BMKG returned HTTP %d", resp.StatusCode)
		channel <- fetchresult[[]TsunamiWarning]{err: err, result: nil}
		return
	}
	if err := json.NewDecoder(resp.Body).Decode(&warnings); err != nil {
		log.Printf("Error decoding JSON: %v", err)
		channel <- fetchresult[[]TsunamiWarning]{err: err, result: nil}
		return
	}
	channel <- fetchresult[[]TsunamiWarning]{err: nil, result: warnings}
}
func poll(since time.Time) ([]SeismicEvent, []TsunamiWarning, error) {
	chan1 := make(chan fetchresult[[]SeismicEvent])
	chan2 := make(chan fetchresult[[]TsunamiWarning])
	go fetchSeismicEvent(since, chan1)
	go fetchTsunamiWarning(since, chan2)
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
func normalize(data SeismicEvent, warnings []TsunamiWarning) hazard.Hazard {
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

	attribute, err := structpb.NewStruct(attributemap)
	if err != nil {
		println(err.Error())
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
		Attributes: attribute,
	}
}
func main() {
	interval := pollInterval()
	now := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		events, warnings, _ := poll(now)
		now = time.Now()
		for _, data := range events {
			go func() {
				normalized := normalize(data, warnings)
				SendToAggregate(&normalized)
			}()
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
