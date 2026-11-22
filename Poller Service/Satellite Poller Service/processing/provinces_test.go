package processing

import (
	"testing"
	"time"

	"satellitepoller/domain"
)

func TestResolveProvinceCoversAllMajorRegions(t *testing.T) {
	cases := []struct {
		name string
		lat  float64
		lon  float64
		want string
	}{
		{name: "Aceh", lat: 5.55, lon: 95.32, want: "Aceh"},
		{name: "Riau", lat: 0.51, lon: 101.45, want: "Riau"},
		{name: "Sumatera Selatan", lat: -2.99, lon: 104.76, want: "Sumatera Selatan"},
		{name: "Kalimantan Barat", lat: -0.03, lon: 109.34, want: "Kalimantan Barat"},
		{name: "Kalimantan Tengah", lat: -2.21, lon: 113.92, want: "Kalimantan Tengah"},
		{name: "Kalimantan Timur", lat: -0.50, lon: 117.15, want: "Kalimantan Timur"},
		{name: "Jawa Barat", lat: -6.91, lon: 107.61, want: "Jawa Barat"},
		{name: "Sulawesi Selatan", lat: -5.14, lon: 119.43, want: "Sulawesi Selatan"},
		{name: "Nusa Tenggara Timur", lat: -10.17, lon: 123.61, want: "Nusa Tenggara Timur"},
		{name: "Papua Selatan", lat: -8.49, lon: 140.40, want: "Papua Selatan"},
		{name: "OutOfBounds", lat: 35.67, lon: 139.65, want: fallbackArea},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveProvince(tc.lat, tc.lon); got != tc.want {
				t.Fatalf("ResolveProvince(%v, %v) = %q, want %q", tc.lat, tc.lon, got, tc.want)
			}
		})
	}
}

func TestBuildHazardEventPopulatesAreaName(t *testing.T) {
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	cluster := domain.FireCluster{
		ID:             "sat-test-cluster",
		SourceRefID:    "cluster-anchor-1",
		Status:         domain.ClusterActive,
		Latitude:       -2.21,
		Longitude:      113.92,
		OccurredAt:     now.Add(-5 * time.Minute),
		DetectionCount: 3,
		TotalFRPMW:     90.5,
		Severity:       "SIAGA",
		Satellites:     []string{"NOAA-20", "VIIRS"},
		MaxConfidence:  "high",
		DetectionKeys:  []string{"k1", "k2", "k3"},
		Revision:       1,
	}

	ev, err := BuildHazardEvent(cluster, now)
	if err != nil {
		t.Fatalf("BuildHazardEvent failed: %v", err)
	}
	if ev.AreaName != "Kalimantan Tengah" {
		t.Fatalf("expected AreaName Kalimantan Tengah, got %q", ev.AreaName)
	}
}
