package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

const (
	coastalSource     = "SENSOR_PANTAI"
	coastalHazardType = "COASTAL"
)

// HazardSeverity uses the enum names from the canonical Hazard protobuf.
// Keep these strings aligned with the enum values in hazard.proto.
type HazardSeverity string

const (
	SeverityNormal  HazardSeverity = "NORMAL"
	SeverityWaspada HazardSeverity = "WASPADA"
	SeveritySiaga   HazardSeverity = "SIAGA"
	SeverityAwas    HazardSeverity = "AWAS"
)

// HazardEvent is the canonical, protobuf-JSON-compatible representation
// emitted by the Sensor Pantai adapter. Its field names and enum strings match
// the Hazard protobuf JSON representation used by the existing pollers.
//
// When the Sensor Pantai service adds generated protobuf bindings, this type
// can be replaced by the generated Hazard type without changing the mapping
// rules below.
type HazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    HazardSeverity `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}

// NormalizeObservation converts a validated Sensor Pantai observation into a
// canonical COASTAL HazardEvent.
//
// The caller must only call this after ValidateObservation succeeds. The
// observation_id is used as source_ref_id as specified by the current guide;
// verify that observation_id is unique across the Sensor Pantai source before
// relying on it as the storage/upsert idempotency key.
//
// Severity policy (application-level, not defined by Lampiran C):
//   - critical=true  -> AWAS
//   - critical=false -> WASPADA
//
// Critical notification routing must still use Attributes["critical"] (or
// the typed source value), not severity alone.
func NormalizeObservation(reading CoastalReading, ingestedAt time.Time) (HazardEvent, error) {
	if strings.TrimSpace(reading.ObservationID) == "" {
		return HazardEvent{}, fmt.Errorf("cannot normalize observation: observation ID is empty")
	}
	if strings.TrimSpace(reading.StationID) == "" {
		return HazardEvent{}, fmt.Errorf("cannot normalize observation %q: station ID is empty", reading.ObservationID)
	}
	if reading.ObservedAt.IsZero() {
		return HazardEvent{}, fmt.Errorf("cannot normalize observation %q: observed_at is zero", reading.ObservationID)
	}
	if ingestedAt.IsZero() {
		return HazardEvent{}, fmt.Errorf("cannot normalize observation %q: ingested_at is zero", reading.ObservationID)
	}
	if reading.Station.Latitude == nil || reading.Station.Longitude == nil {
		return HazardEvent{}, fmt.Errorf("cannot normalize observation %q: station coordinates are missing", reading.ObservationID)
	}

	province := strings.TrimSpace(reading.Station.Province)
	if province == "" {
		return HazardEvent{}, fmt.Errorf("cannot normalize observation %q: station province is empty", reading.ObservationID)
	}

	sourceRefID := strings.TrimSpace(reading.ObservationID)
	hazardID := deterministicHazardID(coastalSource, sourceRefID)

	severity := SeverityWaspada
	if reading.Critical {
		severity = SeverityAwas
	}

	attributes := map[string]any{
		"station_id":          reading.StationID,
		"critical":            reading.Critical,
		"quality":             reading.Quality,
		"sea_level_anomaly_m": reading.SeaLevelAnomalyM,
		"wave_height_m":       reading.WaveHeightM,
	}

	event := HazardEvent{
		HazardID:    hazardID,
		Source:      coastalSource,
		SourceRefID: sourceRefID,
		HazardType:  coastalHazardType,
		Severity:    severity,
		AreaName:    province,
		Latitude:    *reading.Station.Latitude,
		Longitude:   *reading.Station.Longitude,
		OccurredAt:  reading.ObservedAt.UTC(),
		IngestedAt:  ingestedAt.UTC(),
		Attributes:  attributes,
	}

	return event, nil
}

// deterministicHazardID generates a stable ID for the same source reference.
// A hash avoids exposing arbitrary source identifiers and keeps the ID format
// fixed in length. Do not change the input composition after data is persisted,
// otherwise replayed observations could receive a different hazard_id.
func deterministicHazardID(source, sourceRefID string) string {
	identity := source + "\x00" + sourceRefID
	digest := sha256.Sum256([]byte(identity))
	return "coastal-" + hex.EncodeToString(digest[:])
}
