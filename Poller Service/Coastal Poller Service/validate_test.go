package main

import (
	"testing"
	"time"
)

func floatPtr(v float64) *float64  { return &v }
func stringPtr(v string) *string   { return &v }
func boolPtr(v bool) *bool         { return &v }
func timePtr(v time.Time) *time.Time { return &v }

func testStations() map[string]Station {
	return map[string]Station{
		"stn-padang-01": {
			StationID: "stn-padang-01",
			Name:      "Padang",
			Province:  "Sumatera Barat",
			Latitude:  floatPtr(-0.9471),
			Longitude: floatPtr(100.3543),
		},
	}
}

func testPolicy() ValidationPolicy {
	return ValidationPolicy{
		MaxObservationAge: 30 * time.Minute,
		MaxFutureSkew:     0,
	}
}

func TestValidateObservationAcceptsSensorClockAndValidFields(t *testing.T) {
	sensorNow := time.Date(2026, 11, 13, 15, 30, 0, 0, time.UTC)
	obsTime := sensorNow.Add(-5 * time.Minute)
	obs := &Observation{
		ObservationID:    "obs-0001",
		StationID:        "stn-padang-01",
		ObservedAt:       timePtr(obsTime),
		SeaLevelAnomalyM: floatPtr(-0.25),
		WaveHeightM:      floatPtr(1.2),
		Quality:          stringPtr("ok"),
		Critical:         boolPtr(true),
	}

	reading, reason := ValidateObservation(obs, testStations(), testPolicy(), sensorNow)
	if reason != "" {
		t.Fatalf("expected valid observation, got reason %q", reason)
	}
	if reading.Quality != "ok" || !reading.Critical {
		t.Fatalf("unexpected reading fields: %+v", reading)
	}
}

func TestValidateObservationRejectsSuspectQualityAndNegativeWaveHeight(t *testing.T) {
	sensorNow := time.Date(2026, 11, 13, 15, 30, 0, 0, time.UTC)

	suspectObs := &Observation{
		ObservationID:    "obs-suspect",
		StationID:        "stn-padang-01",
		ObservedAt:       timePtr(sensorNow.Add(-1 * time.Minute)),
		SeaLevelAnomalyM: floatPtr(0.1),
		WaveHeightM:      floatPtr(0.8),
		Quality:          stringPtr("suspect"),
		Critical:         boolPtr(false),
	}
	if _, reason := ValidateObservation(suspectObs, testStations(), testPolicy(), sensorNow); reason != ReasonInvalidQuality {
		t.Fatalf("expected %q for suspect quality, got %q", ReasonInvalidQuality, reason)
	}

	negWaveObs := &Observation{
		ObservationID:    "obs-neg-wave",
		StationID:        "stn-padang-01",
		ObservedAt:       timePtr(sensorNow.Add(-1 * time.Minute)),
		SeaLevelAnomalyM: floatPtr(0.1),
		WaveHeightM:      floatPtr(-0.5),
		Quality:          stringPtr("ok"),
		Critical:         boolPtr(false),
	}
	if _, reason := ValidateObservation(negWaveObs, testStations(), testPolicy(), sensorNow); reason != ReasonNegativeWaveHeight {
		t.Fatalf("expected %q for negative wave_height_m, got %q", ReasonNegativeWaveHeight, reason)
	}
}

func TestValidateObservationFreshnessAndPageCheckWithSensorClock(t *testing.T) {
	sensorNow := time.Date(2026, 11, 13, 15, 30, 0, 0, time.UTC)
	wallClock := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

	staleObs := &Observation{
		ObservationID:    "obs-stale",
		StationID:        "stn-padang-01",
		ObservedAt:       timePtr(sensorNow.Add(-45 * time.Minute)),
		SeaLevelAnomalyM: floatPtr(0.1),
		WaveHeightM:      floatPtr(0.5),
		Quality:          stringPtr("ok"),
		Critical:         boolPtr(false),
	}
	if _, reason := ValidateObservation(staleObs, testStations(), testPolicy(), sensorNow); reason != ReasonStaleObservation {
		t.Fatalf("expected %q, got %q", ReasonStaleObservation, reason)
	}

	futureObs := &Observation{
		ObservationID:    "obs-future",
		StationID:        "stn-padang-01",
		ObservedAt:       timePtr(sensorNow.Add(1 * time.Minute)),
		SeaLevelAnomalyM: floatPtr(0.1),
		WaveHeightM:      floatPtr(0.5),
		Quality:          stringPtr("ok"),
		Critical:         boolPtr(false),
	}
	if _, reason := ValidateObservation(futureObs, testStations(), testPolicy(), sensorNow); reason != ReasonFutureTimestamp {
		t.Fatalf("expected %q, got %q", ReasonFutureTimestamp, reason)
	}

	page := ObservationPage{
		GeneratedAt: timePtr(sensorNow),
		Items: []ObservationRecord{
			{Observation: staleObs},
			{Observation: &Observation{
				ObservationID:    "obs-fresh",
				StationID:        "stn-padang-01",
				ObservedAt:       timePtr(sensorNow.Add(-5 * time.Minute)),
				SeaLevelAnomalyM: floatPtr(0.2),
				WaveHeightM:      floatPtr(0.7),
				Quality:          stringPtr("ok"),
				Critical:         boolPtr(false),
			}},
		},
	}

	ref := observationReferenceTime(page, wallClock)
	if !ref.Equal(sensorNow) {
		t.Fatalf("expected reference time %v, got %v", sensorNow, ref)
	}
	if !pageHasFreshObservation(page.Items, ref, testPolicy()) {
		t.Fatalf("expected page to be considered fresh relative to page.GeneratedAt")
	}

	emptyPage := ObservationPage{GeneratedAt: nil}
	if got := observationReferenceTime(emptyPage, wallClock); !got.Equal(wallClock) {
		t.Fatalf("expected wall clock fallback %v, got %v", wallClock, got)
	}
}
