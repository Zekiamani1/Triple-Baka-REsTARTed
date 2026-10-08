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

func SendToAggregate(thething *proto.Hazard) {
	payload, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(thething)
	if err != nil {
		log.Printf("Failed to encode HazardEvent: %v", err)
		return
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP("kafka-aggregate:29092"),
		Topic:    "hazard-ingest",
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

func poll(since time.Time) (reports []VolcanoReport, err error) {
	pvmbg := "http://pvmbg:6767"

	targeturl := fmt.Sprintf("%s/volcanic-reports?since=%s", pvmbg, url.QueryEscape(since.Format(time.RFC3339)))
	resp, err := http.Get(targeturl)

	if err != nil {
		log.Print(err.Error())
		return
	}
	defer resp.Body.Close()
	a, err := io.ReadAll(resp.Body)
	println(string(a))

	if err = json.UnmarshalRead(bytes.NewReader(a), &reports); err != nil {
		log.Printf("Error decoding JSON: %v", err)
		return
	}
	a, err = json.Marshal(reports[0])
	println(string(a))
	return
}

func normalize(data VolcanoReport) proto.Hazard {
	attribute, err := structpb.NewStruct(data.Extra)
	if err != nil {
		println(err.Error())
	}
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
		Longitude:   0,
		Latitude:    0, //belum
		Severity:    severity,
		AreaName:    data.VolcanoID,
		OccurredAt:  timestamppb.New(data.ReportedAt),
		IngestedAt:  timestamppb.New(time.Now()),
		Attributes:  attribute,
	}
}
func main() {
	interval := pollInterval()
	now := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		events, _ := poll(now)
		now = time.Now()
		for _, data := range events {
			go func() {
				normalized := normalize(data)
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
