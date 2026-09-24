package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"bmkg-poller/hazard"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func SendToAggregate(thething *hazard.Hazard) {
	aggregateAddr := "localhost:50051"
	conn, err := grpc.NewClient(aggregateAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	client := hazard.NewAggregateClient(conn)
	client.AggregateEvent(context.Background(), thething)
}
func poll(since time.Time) []SeismicEvent {
	bmkgadress := "http://localhost:6767"

	targeturl := fmt.Sprintf("%s/seismic-events?since=%s", bmkgadress, url.QueryEscape(since.Format(time.RFC3339)))
	resp, err := http.Get(targeturl)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer resp.Body.Close()
	var raw []SeismicEvent
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		log.Printf("Error decoding JSON: %v", err)
		return nil
	}
	return raw
}
func normalize(data SeismicEvent) hazard.Hazard {
	attribute, _ := structpb.NewStruct(
		map[string]any{
			"magnitude":         data.Magnitude,
			"depth_km":          data.DepthKM,
			"potential_tsunami": data.PotentialTsunami,
		})
	return hazard.Hazard{

		HazardId:    uuid.NewString(),
		Source:      hazard.Source_BMKG,
		SourceRefId: data.EventID,
		HazardType:  hazard.HazardType_SEISMIC,

		Severity: hazard.Severity_WASPADA, //belum

		AreaName:   data.RegionName,
		Latitude:   data.EpicenterLat,
		Longitude:  data.EpicenterLon,
		OccurredAt: timestamppb.New(data.OccurredAt),
		IngestedAt: timestamppb.New(time.Now()),
		Attributes: attribute,
	}
}
func main() {
	now := time.Now()
	for true {
		time.Sleep(3 * time.Second)
		datas := poll(now)
		now = time.Now()
		for _, data := range datas {
			normalized := normalize(data)
			SendToAggregate(&normalized)
		}
	}
}
