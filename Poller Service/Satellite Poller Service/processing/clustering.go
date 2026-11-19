package processing

import (
	"math"
	"sort"
	"time"

	"satellitepoller/domain"
)

const (
	maxClusterDistanceKM = 1.0
	maxClusterTimeGap    = 360 * time.Minute
	earthRadiusKM        = 6371.0088
)

type gridCell struct{ X, Y, Z int64 }

func cellFor(latitude, longitude float64) gridCell {
	lat := latitude * math.Pi / 180
	lon := longitude * math.Pi / 180
	x := earthRadiusKM * math.Cos(lat) * math.Cos(lon)
	y := earthRadiusKM * math.Cos(lat) * math.Sin(lon)
	z := earthRadiusKM * math.Sin(lat)
	return gridCell{X: int64(math.Floor(x)), Y: int64(math.Floor(y)), Z: int64(math.Floor(z))}
}

func BuildClusters(detections []domain.Detection, now time.Time) ([]domain.FireCluster, int) {
	active := make([]domain.Detection, 0, len(detections))
	for _, d := range detections {
		if d.Status == domain.DetectionActive {
			active = append(active, d)
		}
	}
	sort.Slice(active, func(i, j int) bool { return active[i].Key < active[j].Key })
	if len(active) == 0 {
		return nil, 0
	}

	parent := make([]int, len(active))
	rank := make([]uint8, len(active))
	for i := range parent {
		parent[i] = i
	}

	buckets := make(map[gridCell][]int, len(active))
	for i, detection := range active {
		cell := cellFor(detection.Latitude, detection.Longitude)
		for dx := -1; dx <= 1; dx++ {
			for dy := -1; dy <= 1; dy++ {
				for dz := -1; dz <= 1; dz++ {
					for _, j := range buckets[gridCell{X: cell.X + int64(dx), Y: cell.Y + int64(dy), Z: cell.Z + int64(dz)}] {
						if directlyConnected(detection, active[j]) {
							union(parent, rank, i, j)
						}
					}
				}
			}
		}
		buckets[cell] = append(buckets[cell], i)
	}

	components := make(map[int][]domain.Detection)
	for i, d := range active {
		root := find(parent, i)
		components[root] = append(components[root], d)
	}

	clusters := make([]domain.FireCluster, 0, len(components))
	singleCount := 0
	for _, members := range components {
		if len(members) < 2 {
			singleCount++
			continue
		}
		clusters = append(clusters, summarizeCluster(members, now.UTC()))
	}
	sort.Slice(clusters, func(i, j int) bool { return clusters[i].SourceRefID < clusters[j].SourceRefID })
	return clusters, singleCount
}

func directlyConnected(a, b domain.Detection) bool {
	delta := a.EventTime.Sub(b.EventTime)
	if delta < 0 {
		delta = -delta
	}
	if delta > maxClusterTimeGap {
		return false
	}
	return haversineKM(a.Latitude, a.Longitude, b.Latitude, b.Longitude) <= maxClusterDistanceKM
}

func haversineKM(lat1, lon1, lat2, lon2 float64) float64 {
	toRadians := func(degrees float64) float64 { return degrees * math.Pi / 180 }
	dLat := toRadians(lat2 - lat1)
	dLon := toRadians(lon2 - lon1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(toRadians(lat1))*math.Cos(toRadians(lat2))*
			math.Sin(dLon/2)*math.Sin(dLon/2)
	if a > 1 {
		a = 1
	}
	return 2 * earthRadiusKM * math.Asin(math.Sqrt(a))
}

func find(parent []int, x int) int {
	if parent[x] != x {
		parent[x] = find(parent, parent[x])
	}
	return parent[x]
}

func union(parent []int, rank []uint8, a, b int) {
	ra, rb := find(parent, a), find(parent, b)
	if ra == rb {
		return
	}
	if rank[ra] < rank[rb] {
		parent[ra] = rb
		return
	}
	if rank[ra] > rank[rb] {
		parent[rb] = ra
		return
	}
	parent[rb] = ra
	rank[ra]++
}

func summarizeCluster(members []domain.Detection, now time.Time) domain.FireCluster {
	sort.Slice(members, func(i, j int) bool { return members[i].Key < members[j].Key })
	var sumLat, sumLon, totalFRP float64
	occurredAt := members[0].EventTime
	satellites := make(map[string]struct{})
	keys := make([]string, 0, len(members))
	maxConfidence := ""
	maxRank := -1
	for _, d := range members {
		sumLat += d.Latitude
		sumLon += d.Longitude
		totalFRP += d.FRPMW
		if d.EventTime.Before(occurredAt) {
			occurredAt = d.EventTime
		}
		satellites[d.Satellite] = struct{}{}
		keys = append(keys, d.Key)
		if r := confidenceRank(d.Confidence); r > maxRank {
			maxRank, maxConfidence = r, d.Confidence
		}
	}
	satelliteList := make([]string, 0, len(satellites))
	for satellite := range satellites {
		satelliteList = append(satelliteList, satellite)
	}
	sort.Strings(satelliteList)

	anchor := keys[0]
	id := stableFireID(anchor)
	return domain.FireCluster{
		ID: id, SourceRefID: id, Status: domain.ClusterActive, AnchorKey: anchor,
		Latitude: sumLat / float64(len(members)), Longitude: sumLon / float64(len(members)),
		OccurredAt: occurredAt.UTC(), DetectionCount: len(members), TotalFRPMW: totalFRP,
		Severity: severityFor(len(members), totalFRP), Satellites: satelliteList,
		MaxConfidence: maxConfidence, DetectionKeys: keys, Revision: 0, UpdatedAt: now,
	}
}
