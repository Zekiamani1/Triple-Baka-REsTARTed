package processing

import "math"

type provinceSeed struct {
	name string
	lat  float64
	lon  float64
}

const (
	indonesiaMinLat = -12.0
	indonesiaMaxLat = 7.0
	indonesiaMinLon = 94.0
	indonesiaMaxLon = 142.0
	fallbackArea    = "Luar Wilayah Indonesia"
)

// indonesiaProvinceSeeds covers all 38 Indonesian provinces with multiple
// geographic anchors for elongated/archipelagic provinces so nearest-anchor
// lookup accurately resolves hotspots across Sumatra, Java, Kalimantan,
// Sulawesi, Bali/Nusa Tenggara, Maluku, and Papua.
var indonesiaProvinceSeeds = []provinceSeed{
	// Aceh & Sumatra
	{name: "Aceh", lat: 5.55, lon: 95.32},
	{name: "Aceh", lat: 4.55, lon: 96.75},
	{name: "Aceh", lat: 3.25, lon: 97.35},
	{name: "Sumatera Utara", lat: 3.59, lon: 98.67},
	{name: "Sumatera Utara", lat: 2.02, lon: 98.98},
	{name: "Sumatera Utara", lat: 1.48, lon: 99.65},
	{name: "Sumatera Barat", lat: -0.95, lon: 100.35},
	{name: "Sumatera Barat", lat: 0.12, lon: 100.10},
	{name: "Sumatera Barat", lat: -1.55, lon: 101.20},
	{name: "Riau", lat: 0.51, lon: 101.45},
	{name: "Riau", lat: 1.67, lon: 101.45},
	{name: "Riau", lat: -0.45, lon: 102.55},
	{name: "Kepulauan Riau", lat: 1.08, lon: 104.03},
	{name: "Kepulauan Riau", lat: 3.95, lon: 108.38},
	{name: "Jambi", lat: -1.61, lon: 103.61},
	{name: "Jambi", lat: -1.80, lon: 102.25},
	{name: "Bengkulu", lat: -3.79, lon: 102.26},
	{name: "Bengkulu", lat: -2.85, lon: 101.60},
	{name: "Sumatera Selatan", lat: -2.99, lon: 104.76},
	{name: "Sumatera Selatan", lat: -3.45, lon: 103.85},
	{name: "Sumatera Selatan", lat: -2.35, lon: 104.05},
	{name: "Kepulauan Bangka Belitung", lat: -2.13, lon: 106.11},
	{name: "Kepulauan Bangka Belitung", lat: -2.74, lon: 107.64},
	{name: "Lampung", lat: -5.43, lon: 105.27},
	{name: "Lampung", lat: -4.45, lon: 105.05},

	// Java & Banten
	{name: "Banten", lat: -6.12, lon: 106.15},
	{name: "Banten", lat: -6.60, lon: 105.85},
	{name: "DKI Jakarta", lat: -6.20, lon: 106.85},
	{name: "Jawa Barat", lat: -6.91, lon: 107.61},
	{name: "Jawa Barat", lat: -6.72, lon: 108.55},
	{name: "Jawa Barat", lat: -7.25, lon: 106.95},
	{name: "Jawa Tengah", lat: -7.00, lon: 110.42},
	{name: "Jawa Tengah", lat: -7.42, lon: 109.24},
	{name: "Jawa Tengah", lat: -6.95, lon: 111.15},
	{name: "DI Yogyakarta", lat: -7.80, lon: 110.37},
	{name: "Jawa Timur", lat: -7.26, lon: 112.75},
	{name: "Jawa Timur", lat: -7.98, lon: 112.63},
	{name: "Jawa Timur", lat: -7.85, lon: 111.45},
	{name: "Jawa Timur", lat: -8.17, lon: 113.70},

	// Bali & Nusa Tenggara
	{name: "Bali", lat: -8.41, lon: 115.19},
	{name: "Nusa Tenggara Barat", lat: -8.58, lon: 116.12},
	{name: "Nusa Tenggara Barat", lat: -8.54, lon: 118.72},
	{name: "Nusa Tenggara Timur", lat: -10.17, lon: 123.61},
	{name: "Nusa Tenggara Timur", lat: -8.62, lon: 122.21},
	{name: "Nusa Tenggara Timur", lat: -9.65, lon: 120.26},

	// Kalimantan
	{name: "Kalimantan Barat", lat: -0.03, lon: 109.34},
	{name: "Kalimantan Barat", lat: 0.85, lon: 112.40},
	{name: "Kalimantan Barat", lat: -1.85, lon: 110.25},
	{name: "Kalimantan Tengah", lat: -2.21, lon: 113.92},
	{name: "Kalimantan Tengah", lat: -1.15, lon: 113.45},
	{name: "Kalimantan Tengah", lat: -2.45, lon: 111.75},
	{name: "Kalimantan Tengah", lat: -1.55, lon: 114.85},
	{name: "Kalimantan Selatan", lat: -3.32, lon: 114.59},
	{name: "Kalimantan Selatan", lat: -2.65, lon: 115.40},
	{name: "Kalimantan Timur", lat: -0.50, lon: 117.15},
	{name: "Kalimantan Timur", lat: -1.27, lon: 116.83},
	{name: "Kalimantan Timur", lat: 1.45, lon: 117.45},
	{name: "Kalimantan Utara", lat: 3.31, lon: 117.59},
	{name: "Kalimantan Utara", lat: 2.84, lon: 116.70},

	// Sulawesi
	{name: "Sulawesi Utara", lat: 1.49, lon: 124.84},
	{name: "Gorontalo", lat: 0.54, lon: 123.06},
	{name: "Sulawesi Tengah", lat: -0.90, lon: 119.87},
	{name: "Sulawesi Tengah", lat: -1.25, lon: 121.65},
	{name: "Sulawesi Barat", lat: -2.68, lon: 118.89},
	{name: "Sulawesi Selatan", lat: -5.14, lon: 119.43},
	{name: "Sulawesi Selatan", lat: -3.55, lon: 120.15},
	{name: "Sulawesi Tenggara", lat: -3.99, lon: 122.51},
	{name: "Sulawesi Tenggara", lat: -5.45, lon: 122.62},

	// Maluku
	{name: "Maluku Utara", lat: 0.79, lon: 127.36},
	{name: "Maluku Utara", lat: 1.65, lon: 127.90},
	{name: "Maluku", lat: -3.70, lon: 128.18},
	{name: "Maluku", lat: -5.65, lon: 132.75},

	// Papua (all 6 provinces)
	{name: "Papua Barat Daya", lat: -0.88, lon: 131.25},
	{name: "Papua Barat", lat: -0.86, lon: 134.06},
	{name: "Papua Barat", lat: -2.95, lon: 132.30},
	{name: "Papua Tengah", lat: -3.37, lon: 135.50},
	{name: "Papua Tengah", lat: -4.55, lon: 136.89},
	{name: "Papua", lat: -2.53, lon: 140.72},
	{name: "Papua", lat: -1.75, lon: 136.25},
	{name: "Papua Pegunungan", lat: -4.08, lon: 138.95},
	{name: "Papua Selatan", lat: -8.49, lon: 140.40},
	{name: "Papua Selatan", lat: -6.15, lon: 139.65},
}

// ResolveProvince maps WGS-84 coordinates inside Indonesia to the nearest
// Indonesian province using haversine distance to regional reference anchors.
func ResolveProvince(lat, lon float64) string {
	if math.IsNaN(lat) || math.IsInf(lat, 0) || math.IsNaN(lon) || math.IsInf(lon, 0) {
		return fallbackArea
	}
	if lat < indonesiaMinLat || lat > indonesiaMaxLat || lon < indonesiaMinLon || lon > indonesiaMaxLon {
		return fallbackArea
	}

	bestName := fallbackArea
	bestDist := math.MaxFloat64
	for _, seed := range indonesiaProvinceSeeds {
		d := haversineKM(lat, lon, seed.lat, seed.lon)
		if d < bestDist {
			bestDist = d
			bestName = seed.name
		}
	}
	return bestName
}
