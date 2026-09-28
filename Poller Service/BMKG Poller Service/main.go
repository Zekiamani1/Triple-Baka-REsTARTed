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
	aggregateAddr := "aggregate:50051"
	conn, err := grpc.NewClient(aggregateAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	client := hazard.NewAggregateClient(conn)
	client.AggregateEvent(context.Background(), thething)
}

type fetchresult[T any] struct {
	err    error
	result T
}

func fetchSeismicEvent(since time.Time, channel chan fetchresult[[]SeismicEvent]) {
	var events []SeismicEvent
	bmkgadress := "http://bmkg:6767"

	targeturl := fmt.Sprintf("%s/seismic-events?since=%s", bmkgadress, url.QueryEscape(since.Format(time.RFC3339)))
	resp, err := http.Get(targeturl)

	if err != nil {
		log.Print(err.Error())
		channel <- fetchresult[[]SeismicEvent]{err: err, result: nil}
		return
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		log.Printf("Error decoding JSON: %v", err)
		channel <- fetchresult[[]SeismicEvent]{err: err, result: nil}
		return
	}
	channel <- fetchresult[[]SeismicEvent]{err: nil, result: events}
}
func fetchTsunamiWarning(since time.Time, channel chan fetchresult[[]TsunamiWarning]) {
	var warnings []TsunamiWarning
	bmkgadress := "http://bmkg:6767"

	targeturl := fmt.Sprintf("%s/tsunami-warnings?since=%s", bmkgadress, url.QueryEscape(since.Format(time.RFC3339)))
	resp, err := http.Get(targeturl)
	if err != nil {
		log.Print(err.Error())
		channel <- fetchresult[[]TsunamiWarning]{err: err, result: nil}
		return
	}
	defer resp.Body.Close()
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
			}
			break
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
		case 5.0 <= data.Magnitude && data.Magnitude <= 6.5:
			severity = hazard.Severity_WASPADA
		case 6.5 < data.Magnitude:
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
	sleeptime := 3
	now := time.Now()
	for true {
		time.Sleep(time.Duration(sleeptime) * time.Second)
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
