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
	"time"

	"pvmbgpoll/proto"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	aggregateAddr := "localhost:50051"
	conn, err := grpc.NewClient(aggregateAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	client := proto.NewAggregateClient(conn)
	client.AggregateEvent(context.Background(), thething)
}

type fetchresult[T any] struct {
	err    error
	result T
}

func poll(since time.Time) (reports []VolcanoReport, err error) {
	pvmbg := "http://localhost:6767"

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
		Source:      proto.Source_BMKG,
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
	sleeptime := 3
	now := time.Now()
	for true {
		time.Sleep(time.Duration(sleeptime) * time.Second)
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
