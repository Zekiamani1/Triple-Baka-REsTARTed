package processing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"satellitepoller/domain"
)

var ErrProvinceReferenceMissing = errors.New("province reference data is missing")

type OutboxEnvelope struct {
	Operation   string              `json:"operation"`
	SourceRefID string              `json:"source_ref_id"`
	Revision    int64               `json:"revision"`
	Hazard      *domain.HazardEvent `json:"hazard,omitempty"`
	Reason      string              `json:"reason,omitempty"`
}

func BuildHazardEvent(cluster domain.FireCluster, provinces []ProvinceReference, now time.Time) (domain.HazardEvent, error) {
	area, err := nearestProvince(cluster.Latitude, cluster.Longitude, provinces)
	if err != nil {
		return domain.HazardEvent{}, err
	}
	keys := append([]string(nil), cluster.DetectionKeys...)
	sort.Strings(keys)
	satellites := append([]string(nil), cluster.Satellites...)
	sort.Strings(satellites)

	satelliteValues := make([]any, len(satellites))
	for i, value := range satellites {
		satelliteValues[i] = value
	}
	detectionKeyValues := make([]any, len(keys))
	for i, value := range keys {
		detectionKeyValues[i] = value
	}

	return domain.HazardEvent{
		HazardID:    cluster.ID,
		Source:      "SATELIT",
		SourceRefID: cluster.SourceRefID,
		HazardType:  "WILDFIRE",
		Severity:    cluster.Severity,
		AreaName:    area,
		Latitude:    cluster.Latitude,
		Longitude:   cluster.Longitude,
		OccurredAt:  cluster.OccurredAt.UTC(),
		IngestedAt:  now.UTC(),
		Attributes: map[string]any{
			"detection_count": cluster.DetectionCount,
			"satellites":      satelliteValues,
			"max_confidence":  cluster.MaxConfidence,
			"total_frp_mw":    cluster.TotalFRPMW,
			"detection_keys":  detectionKeyValues,
		},
	}, nil
}

// ReconcileClusters compares the freshly computed components with persisted
// clusters. It preserves an existing cluster ID when its deterministic anchor
// remains part of that component, increments revision only when content changes,
// and creates retract envelopes for clusters that disappeared or were superseded.
func ReconcileClusters(oldClusters, computed []domain.FireCluster, provinces []ProvinceReference, now time.Time) ([]domain.FireCluster, []domain.OutboxEvent, error) {
	now = now.UTC()
	oldActive := make(map[string]domain.FireCluster)
	allByID := make(map[string]domain.FireCluster)
	for _, old := range oldClusters {
		allByID[old.ID] = old
		if old.Status == domain.ClusterActive {
			oldActive[old.ID] = old
		}
	}

	newByID := make(map[string]domain.FireCluster, len(computed))
	for _, cluster := range computed {
		newByID[cluster.ID] = cluster
	}

	out := make([]domain.FireCluster, 0, len(allByID)+len(computed))
	outbox := make([]domain.OutboxEvent, 0)
	for id, cluster := range newByID {
		previous, wasActive := oldActive[id]
		if wasActive {
			cluster.Revision = previous.Revision
			if !sameClusterContent(previous, cluster) {
				cluster.Revision++
				payload, err := makeOutboxPayload("upsert", cluster, provinces, now, "")
				if err != nil {
					return nil, nil, err
				}
				outbox = append(outbox, newOutboxEvent(cluster, payload, now))
			}
		} else {
			// If an identical deterministic ID existed historically but was
			// retracted/superseded, continue its revision sequence rather than
			// reusing the old outbox idempotency key.
			if previous, existed := allByID[id]; existed {
				cluster.Revision = previous.Revision + 1
			} else {
				cluster.Revision = 1
			}
			payload, err := makeOutboxPayload("upsert", cluster, provinces, now, "")
			if err != nil {
				return nil, nil, err
			}
			outbox = append(outbox, newOutboxEvent(cluster, payload, now))
		}
		cluster.UpdatedAt = now
		out = append(out, cluster)
	}

	for id, previous := range oldActive {
		if _, stillActive := newByID[id]; stillActive {
			continue
		}
		status := domain.ClusterRetracted
		reason := "cluster_no_longer_has_two_active_detections"
		for _, next := range computed {
			if intersects(previous.DetectionKeys, next.DetectionKeys) {
				status = domain.ClusterSuperseded
				reason = "cluster_membership_changed_or_merged"
				break
			}
		}
		previous.Status = status
		previous.Revision++
		previous.UpdatedAt = now
		allByID[id] = previous
		out = append(out, previous)
		payload, err := json.Marshal(OutboxEnvelope{
			Operation: "retract", SourceRefID: previous.SourceRefID,
			Revision: previous.Revision, Reason: reason,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("encode retract outbox payload: %w", err)
		}
		outbox = append(outbox, newOutboxEvent(previous, payload, now))
	}

	// Preserve historical/inactive rows for audit and deterministic recovery.
	for id, old := range allByID {
		if old.Status != domain.ClusterActive {
			if _, replaced := oldActive[id]; replaced {
				continue
			}
			if _, alreadyAdded := newByID[id]; !alreadyAdded {
				out = append(out, old)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	sort.Slice(outbox, func(i, j int) bool {
		if outbox[i].SourceRefID == outbox[j].SourceRefID {
			return outbox[i].Revision < outbox[j].Revision
		}
		return outbox[i].SourceRefID < outbox[j].SourceRefID
	})
	return out, outbox, nil
}

func makeOutboxPayload(operation string, cluster domain.FireCluster, provinces []ProvinceReference, now time.Time, reason string) ([]byte, error) {
	hazard, err := BuildHazardEvent(cluster, provinces, now)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(OutboxEnvelope{
		Operation: operation, SourceRefID: cluster.SourceRefID,
		Revision: cluster.Revision, Hazard: &hazard, Reason: reason,
	})
	if err != nil {
		return nil, fmt.Errorf("encode hazard outbox payload: %w", err)
	}
	return payload, nil
}

func newOutboxEvent(cluster domain.FireCluster, payload []byte, now time.Time) domain.OutboxEvent {
	operation := "upsert"
	var envelope OutboxEnvelope
	if json.Unmarshal(payload, &envelope) == nil && envelope.Operation != "" {
		operation = envelope.Operation
	}
	idHash := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%s", cluster.SourceRefID, cluster.Revision, operation)))
	return domain.OutboxEvent{
		EventID:     hex.EncodeToString(idHash[:]),
		SourceRefID: cluster.SourceRefID,
		Revision:    cluster.Revision,
		Payload:     payload,
		Status:      "pending",
		CreatedAt:   now.UTC(),
	}
}

func sameClusterContent(a, b domain.FireCluster) bool {
	if a.SourceRefID != b.SourceRefID || a.Latitude != b.Latitude || a.Longitude != b.Longitude ||
		a.OccurredAt.UTC() != b.OccurredAt.UTC() || a.DetectionCount != b.DetectionCount ||
		a.TotalFRPMW != b.TotalFRPMW || a.Severity != b.Severity || a.MaxConfidence != b.MaxConfidence {
		return false
	}
	return equalStrings(a.Satellites, b.Satellites) && equalStrings(a.DetectionKeys, b.DetectionKeys)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa, bb := append([]string(nil), a...), append([]string(nil), b...)
	sort.Strings(aa)
	sort.Strings(bb)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}

func intersects(a, b []string) bool {
	set := make(map[string]struct{}, len(a))
	for _, value := range a {
		set[value] = struct{}{}
	}
	for _, value := range b {
		if _, ok := set[value]; ok {
			return true
		}
	}
	return false
}

func stableFireID(anchor string) string {
	h := sha256.Sum256([]byte(anchor))
	return "fire-" + hex.EncodeToString(h[:12])
}

func severityFor(count int, totalFRP float64) string {
	switch {
	case count >= 20 || totalFRP >= 100:
		return "AWAS"
	case count >= 6 || totalFRP >= 30:
		return "SIAGA"
	default:
		return "WASPADA"
	}
}

func confidenceRank(value string) int {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "h", "high":
		return 3
	case "n", "nominal", "medium", "med":
		return 2
	case "l", "low":
		return 1
	default:
		return 0
	}
}

func isLowConfidence(value string) bool {
	return strings.EqualFold(strings.TrimSpace(value), "low") || strings.EqualFold(strings.TrimSpace(value), "l")
}

func nearestProvince(latitude, longitude float64, provinces []ProvinceReference) (string, error) {
	if len(provinces) == 0 {
		return "", ErrProvinceReferenceMissing
	}
	bestName := ""
	bestDistance := math.Inf(1)
	for _, province := range provinces {
		distance := haversineKM(latitude, longitude, province.Latitude, province.Longitude)
		if distance < bestDistance {
			bestDistance, bestName = distance, province.Name
		}
	}
	if strings.TrimSpace(bestName) == "" {
		return "", ErrProvinceReferenceMissing
	}
	return bestName, nil
}
