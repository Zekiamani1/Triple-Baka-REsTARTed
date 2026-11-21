package main

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// ValidationReason is a stable, low-cardinality reason code suitable for
// quarantine records and metrics. Do not use raw payload values as reason codes.
type ValidationReason string

const (
	ReasonMalformedObservation     ValidationReason = "malformed_observation"
	ReasonMissingObservationID     ValidationReason = "missing_observation_id"
	ReasonMissingStationID         ValidationReason = "missing_station_id"
	ReasonMissingObservedAt        ValidationReason = "missing_observed_at"
	ReasonMissingSeaLevelAnomaly   ValidationReason = "missing_sea_level_anomaly"
	ReasonMissingWaveHeight        ValidationReason = "missing_wave_height"
	ReasonMissingQuality           ValidationReason = "missing_quality"
	ReasonMissingCritical          ValidationReason = "missing_critical"
	ReasonInvalidReferenceTime     ValidationReason = "invalid_reference_time"
	ReasonInvalidValidationPolicy  ValidationReason = "invalid_validation_policy"
	ReasonFutureTimestamp          ValidationReason = "future_timestamp"
	ReasonStaleObservation         ValidationReason = "stale_observation"
	ReasonNonFiniteSeaLevelAnomaly ValidationReason = "non_finite_sea_level_anomaly"
	ReasonNonFiniteWaveHeight      ValidationReason = "non_finite_wave_height"
	ReasonUnknownStation           ValidationReason = "unknown_station"
	ReasonInvalidStationMetadata   ValidationReason = "invalid_station_metadata"
)

// ValidationPolicy contains application-level freshness rules. The Sensor
// Pantai contract does not define a maximum observation age or clock-skew
// tolerance, so callers must choose and document these values explicitly.
type ValidationPolicy struct {
	MaxObservationAge time.Duration
	MaxFutureSkew     time.Duration
}

// Validate checks that the policy is explicit and internally consistent.
func (p ValidationPolicy) Validate() error {
	if p.MaxObservationAge <= 0 {
		return fmt.Errorf("maximum observation age must be greater than zero")
	}
	if p.MaxFutureSkew < 0 {
		return fmt.Errorf("maximum future clock skew must not be negative")
	}
	return nil
}

// CoastalReading is a validated observation enriched with its station metadata.
// It is the input expected by normalize.go when constructing a COASTAL event.
type CoastalReading struct {
	ObservationID    string
	StationID        string
	Station          Station
	ObservedAt       time.Time
	SeaLevelAnomalyM float64
	WaveHeightM      float64
	Quality          string
	Critical         bool
}

// ValidateObservationRecord validates one record returned by SensorClient.
// Decode failures are kept separate from the original bytes in
// ObservationRecord.Raw so the caller can quarantine the untouched payload.
func ValidateObservationRecord(
	record ObservationRecord,
	stations map[string]Station,
	policy ValidationPolicy,
	now time.Time,
) (CoastalReading, ValidationReason) {
	if record.DecodeErr != nil || record.Observation == nil {
		return CoastalReading{}, ReasonMalformedObservation
	}

	return ValidateObservation(record.Observation, stations, policy, now)
}

// ValidateObservation validates required fields, station metadata, numeric
// values, and freshness. It deliberately does not impose undocumented physical
// ranges or an undocumented quality enum: the official contract does not
// specify either. Application-specific quality policy can be added separately
// once the accepted values are established from the sensor contract/profile.
//
// IMPORTANT: a validation failure means the item must not become a HazardEvent.
// The caller must still inspect record.Observation.Critical before discarding
// the record: critical notification routing is a separate decision from
// HazardEvent eligibility (see the Problem 5 guide).
func ValidateObservation(
	observation *Observation,
	stations map[string]Station,
	policy ValidationPolicy,
	now time.Time,
) (CoastalReading, ValidationReason) {
	if observation == nil {
		return CoastalReading{}, ReasonMalformedObservation
	}
	if err := policy.Validate(); err != nil {
		return CoastalReading{}, ReasonInvalidValidationPolicy
	}
	if now.IsZero() {
		return CoastalReading{}, ReasonInvalidReferenceTime
	}

	observationID := strings.TrimSpace(observation.ObservationID)
	if observationID == "" {
		return CoastalReading{}, ReasonMissingObservationID
	}

	stationID := strings.TrimSpace(observation.StationID)
	if stationID == "" {
		return CoastalReading{}, ReasonMissingStationID
	}
	if observation.ObservedAt == nil || observation.ObservedAt.IsZero() {
		return CoastalReading{}, ReasonMissingObservedAt
	}
	if observation.SeaLevelAnomalyM == nil {
		return CoastalReading{}, ReasonMissingSeaLevelAnomaly
	}
	if observation.WaveHeightM == nil {
		return CoastalReading{}, ReasonMissingWaveHeight
	}
	if observation.Quality == nil || strings.TrimSpace(*observation.Quality) == "" {
		return CoastalReading{}, ReasonMissingQuality
	}
	if observation.Critical == nil {
		return CoastalReading{}, ReasonMissingCritical
	}

	if math.IsNaN(*observation.SeaLevelAnomalyM) || math.IsInf(*observation.SeaLevelAnomalyM, 0) {
		return CoastalReading{}, ReasonNonFiniteSeaLevelAnomaly
	}
	if math.IsNaN(*observation.WaveHeightM) || math.IsInf(*observation.WaveHeightM, 0) {
		return CoastalReading{}, ReasonNonFiniteWaveHeight
	}

	// Check event-time freshness before station lookup. Otherwise an old
	// observation from an unknown station must not enter the recovery queue if
	// it was already expired on receipt. For recovery, now is the persisted
	// first-receipt time, so waiting for metadata does not invalidate the event.
	observedAt := observation.ObservedAt.UTC()
	referenceTime := now.UTC()
	age := referenceTime.Sub(observedAt)
	if age < -policy.MaxFutureSkew {
		return CoastalReading{}, ReasonFutureTimestamp
	}
	if age > policy.MaxObservationAge {
		return CoastalReading{}, ReasonStaleObservation
	}

	station, ok := stations[stationID]
	if !ok {
		return CoastalReading{}, ReasonUnknownStation
	}
	if reason := validateStationMetadata(stationID, station); reason != "" {
		return CoastalReading{}, reason
	}

	return CoastalReading{
		ObservationID:    observationID,
		StationID:        stationID,
		Station:          station,
		ObservedAt:       observedAt,
		SeaLevelAnomalyM: *observation.SeaLevelAnomalyM,
		WaveHeightM:      *observation.WaveHeightM,
		Quality:          strings.TrimSpace(*observation.Quality),
		Critical:         *observation.Critical,
	}, ""
}

func validateStationMetadata(expectedStationID string, station Station) ValidationReason {
	if strings.TrimSpace(station.StationID) == "" || strings.TrimSpace(station.StationID) != expectedStationID {
		return ReasonInvalidStationMetadata
	}
	if strings.TrimSpace(station.Province) == "" {
		return ReasonInvalidStationMetadata
	}
	if station.Latitude == nil || station.Longitude == nil {
		return ReasonInvalidStationMetadata
	}
	if math.IsNaN(*station.Latitude) || math.IsInf(*station.Latitude, 0) ||
		*station.Latitude < -90 || *station.Latitude > 90 {
		return ReasonInvalidStationMetadata
	}
	if math.IsNaN(*station.Longitude) || math.IsInf(*station.Longitude, 0) ||
		*station.Longitude < -180 || *station.Longitude > 180 {
		return ReasonInvalidStationMetadata
	}
	return ""
}
