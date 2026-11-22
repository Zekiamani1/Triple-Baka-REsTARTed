package processing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

func BuildHazardEvent(cluster domain.FireCluster, now time.Time) (domain.HazardEvent, error) {
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
		AreaName:    ResolveProvince(cluster.Latitude, cluster.Longitude),
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
func ReconcileClusters(
	oldClusters, computed []domain.FireCluster,
	now time.Time,
) ([]domain.FireCluster, []domain.OutboxEvent, error) {

	now = now.UTC()

	// Index all old clusters and collect active ones.
	oldByID := make(map[string]domain.FireCluster)
	oldActive := make([]domain.FireCluster, 0)

	for _, old := range oldClusters {
		// Backward compatibility for clusters created before AnchorKey.
		if old.AnchorKey == "" && len(old.DetectionKeys) > 0 {
			keys := append([]string(nil), old.DetectionKeys...)
			sort.Strings(keys)
			old.AnchorKey = keys[0]
		}

		oldByID[old.ID] = old

		if old.Status == domain.ClusterActive {
			oldActive = append(oldActive, old)
		}
	}

	sort.Slice(oldActive, func(i, j int) bool {
		return oldActive[i].SourceRefID < oldActive[j].SourceRefID
	})

	// Normalize computed clusters.
	for i := range computed {
		sort.Strings(computed[i].DetectionKeys)

		if computed[i].AnchorKey == "" &&
			len(computed[i].DetectionKeys) > 0 {
			computed[i].AnchorKey = computed[i].DetectionKeys[0]
		}

		if computed[i].AnchorKey == "" {
			return nil, nil, fmt.Errorf(
				"computed cluster has no identity anchor",
			)
		}
	}

	sort.Slice(computed, func(i, j int) bool {
		return firstDetectionKey(computed[i]) <
			firstDetectionKey(computed[j])
	})

	// Each old cluster chooses exactly one resulting component.
	// This is what makes split handling deterministic.
	assigned := make([][]domain.FireCluster, len(computed))

	for _, old := range oldActive {
		bestIndex := -1
		bestContainsAnchor := false
		bestOverlap := 0
		bestKey := ""

		for i, next := range computed {
			overlap := intersectionCount(
				old.DetectionKeys,
				next.DetectionKeys,
			)
			if overlap == 0 {
				continue
			}

			containsAnchor := hasString(
				next.DetectionKeys,
				old.AnchorKey,
			)
			key := firstDetectionKey(next)

			better := bestIndex == -1

			if !better && containsAnchor != bestContainsAnchor {
				better = containsAnchor
			} else if !better && overlap != bestOverlap {
				better = overlap > bestOverlap
			} else if !better &&
				containsAnchor == bestContainsAnchor &&
				overlap == bestOverlap {
				better = key < bestKey
			}

			if better {
				bestIndex = i
				bestContainsAnchor = containsAnchor
				bestOverlap = overlap
				bestKey = key
			}
		}

		if bestIndex >= 0 {
			assigned[bestIndex] = append(
				assigned[bestIndex],
				old,
			)
		}
	}

	result := make([]domain.FireCluster, 0)
	outbox := make([]domain.OutboxEvent, 0)

	// Tracks old identities that survived reconciliation.
	usedOld := make(map[string]bool)

	// Tracks cluster IDs already added to the result.
	outputIDs := make(map[string]bool)

	for i, next := range computed {
		candidates := assigned[i]

		// Merge handling: choose one identity to survive.
		sort.Slice(candidates, func(a, b int) bool {
			left := candidates[a]
			right := candidates[b]

			leftHasAnchor := hasString(
				next.DetectionKeys,
				left.AnchorKey,
			)
			rightHasAnchor := hasString(
				next.DetectionKeys,
				right.AnchorKey,
			)

			if leftHasAnchor != rightHasAnchor {
				return leftHasAnchor
			}

			leftOverlap := intersectionCount(
				left.DetectionKeys,
				next.DetectionKeys,
			)
			rightOverlap := intersectionCount(
				right.DetectionKeys,
				next.DetectionKeys,
			)

			if leftOverlap != rightOverlap {
				return leftOverlap > rightOverlap
			}

			return left.SourceRefID < right.SourceRefID
		})

		if len(candidates) > 0 {
			// Existing identity survives.
			survivor := candidates[0]

			next.ID = survivor.ID
			next.SourceRefID = survivor.SourceRefID
			next.AnchorKey = survivor.AnchorKey
			next.Revision = survivor.Revision

			usedOld[survivor.ID] = true

			// Only publish an update if cluster content changed.
			if !sameClusterContent(survivor, next) {
				next.Revision++

				payload, err := makeOutboxPayload(
					"upsert",
					next,
					now,
					"",
				)
				if err != nil {
					return nil, nil, err
				}

				outbox = append(
					outbox,
					newOutboxEvent(next, payload, now),
				)
			}

		} else {
			// No old cluster owns this component: new fire identity.
			next.ID = stableFireID(next.AnchorKey)
			next.SourceRefID = next.ID

			// Continue the revision sequence if this identity existed
			// historically and is becoming active again.
			if previous, exists := oldByID[next.ID]; exists {
				next.Revision = previous.Revision + 1
			} else {
				next.Revision = 1
			}

			payload, err := makeOutboxPayload(
				"upsert",
				next,
				now,
				"",
			)
			if err != nil {
				return nil, nil, err
			}

			outbox = append(
				outbox,
				newOutboxEvent(next, payload, now),
			)
		}

		next.Status = domain.ClusterActive
		next.UpdatedAt = now

		result = append(result, next)
		outputIDs[next.ID] = true
	}

	// Retract old identities that did not survive.
	for _, previous := range oldActive {
		if usedOld[previous.ID] {
			continue
		}

		status := domain.ClusterRetracted
		reason := "cluster_no_longer_has_two_active_detections"

		for _, next := range computed {
			if intersects(
				previous.DetectionKeys,
				next.DetectionKeys,
			) {
				status = domain.ClusterSuperseded
				reason = "cluster_membership_changed_or_merged"
				break
			}
		}

		previous.Status = status
		previous.Revision++
		previous.UpdatedAt = now

		result = append(result, previous)
		outputIDs[previous.ID] = true

		payload, err := json.Marshal(OutboxEnvelope{
			Operation:   "retract",
			SourceRefID: previous.SourceRefID,
			Revision:    previous.Revision,
			Reason:      reason,
		})
		if err != nil {
			return nil, nil, fmt.Errorf(
				"encode retract outbox payload: %w",
				err,
			)
		}

		outbox = append(
			outbox,
			newOutboxEvent(previous, payload, now),
		)
	}

	// Preserve historical inactive clusters.
	for _, old := range oldClusters {
		if old.Status == domain.ClusterActive {
			continue
		}

		if outputIDs[old.ID] {
			continue
		}

		result = append(result, old)
		outputIDs[old.ID] = true
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	sort.Slice(outbox, func(i, j int) bool {
		if outbox[i].SourceRefID == outbox[j].SourceRefID {
			return outbox[i].Revision < outbox[j].Revision
		}

		return outbox[i].SourceRefID < outbox[j].SourceRefID
	})

	return result, outbox, nil
}

func makeOutboxPayload(operation string, cluster domain.FireCluster, now time.Time, reason string) ([]byte, error) {
	hazard, err := BuildHazardEvent(cluster, now)
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
		a.OccurredAt.UTC() != b.OccurredAt.UTC() || a.DetectionCount != b.DetectionCount || a.AnchorKey != b.AnchorKey ||
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
	return intersectionCount(a, b) > 0
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

func intersectionCount(a, b []string) int {
	set := make(map[string]struct{}, len(a))

	for _, key := range a {
		set[key] = struct{}{}
	}

	count := 0
	for _, key := range b {
		if _, exists := set[key]; exists {
			count++
		}
	}

	return count
}

func hasString(values []string, target string) bool {
	if target == "" {
		return false
	}

	for _, value := range values {
		if value == target {
			return true
		}
	}

	return false
}

func firstDetectionKey(cluster domain.FireCluster) string {
	if len(cluster.DetectionKeys) == 0 {
		return cluster.AnchorKey
	}

	return cluster.DetectionKeys[0]
}
