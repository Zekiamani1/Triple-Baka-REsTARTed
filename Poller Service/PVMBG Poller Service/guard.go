package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

const (
	maxPayloadBytes = 1048576 // 1 MiB
	maxJSONDepth    = 32
)

type dlqRecord struct {
	MessageID   string `json:"message_id"`
	Source      string `json:"source"`
	SourceRefID string `json:"source_ref_id"`
	RawPayload  string `json:"raw_payload"`
	RawEncoding string `json:"raw_encoding"`
	ReceivedAt  string `json:"received_at"`
	Reason      string `json:"reason"`
	Retryable   bool   `json:"retryable"`
	Attempts    int    `json:"attempts"`
	Status      string `json:"status"`
	Seq         int64  `json:"seq"`
}

func readBoundedBody(r io.Reader) ([]byte, bool, error) {
	limited := io.LimitReader(r, maxPayloadBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, false, err
	}
	if len(data) > maxPayloadBytes {
		return data[:maxPayloadBytes], true, nil
	}
	return data, false, nil
}

func jsonMaxDepth(raw []byte) int {
	depth := 0
	maxDepth := 0
	inString := false
	escaped := false
	for _, b := range raw {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if b == '\\' {
				escaped = true
				continue
			}
			if b == '"' {
				inString = false
			}
			continue
		}
		switch b {
		case '"':
			inString = true
		case '{', '[':
			depth++
			if depth > maxDepth {
				maxDepth = depth
			}
		case '}', ']':
			if depth > 0 {
				depth--
			}
		}
	}
	return maxDepth
}

func validatePayloadBytes(raw []byte, oversized bool) (string, bool) {
	if oversized || len(raw) > maxPayloadBytes {
		return "malformed_payload: payload exceeds 1 MiB", false
	}
	if !utf8.Valid(raw) {
		return "malformed_payload: invalid utf-8", false
	}
	if jsonMaxDepth(raw) > maxJSONDepth {
		return fmt.Sprintf("malformed_payload: json nesting depth exceeds %d", maxJSONDepth), false
	}
	return "", true
}

func validateVolcanoReport(raw json.RawMessage) (*VolcanoReport, string, bool) {
	if reason, ok := validatePayloadBytes(raw, false); !ok {
		return nil, reason, false
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, "malformed_payload: record is not a valid JSON object", false
	}

	var reportID string
	if v, exists := fields["report_id"]; !exists || json.Unmarshal(v, &reportID) != nil || strings.TrimSpace(reportID) == "" {
		return nil, "schema_incompatible: missing or invalid report_id", false
	}

	var volcanoID string
	if v, exists := fields["volcano_id"]; !exists || json.Unmarshal(v, &volcanoID) != nil || strings.TrimSpace(volcanoID) == "" {
		return nil, "schema_incompatible: missing or invalid volcano_id", false
	}

	var alertLevel string
	if v, exists := fields["alert_level"]; !exists || json.Unmarshal(v, &alertLevel) != nil {
		return nil, "schema_incompatible: missing or non-string alert_level", false
	}
	switch alertLevel {
	case "Normal", "Waspada", "Siaga", "Awas":
	default:
		return nil, fmt.Sprintf("schema_incompatible: unknown alert_level %q", alertLevel), false
	}

	var eruptionCountFloat float64
	if v, exists := fields["eruption_count_24h"]; !exists || json.Unmarshal(v, &eruptionCountFloat) != nil {
		return nil, "schema_incompatible: missing or non-numeric eruption_count_24h", false
	}
	if eruptionCountFloat != float64(int(eruptionCountFloat)) {
		return nil, "schema_incompatible: eruption_count_24h must be an integer", false
	}

	var ashHeight float64
	if v, exists := fields["ash_column_height_m"]; !exists || json.Unmarshal(v, &ashHeight) != nil {
		return nil, "schema_incompatible: missing or non-numeric ash_column_height_m", false
	}

	var reportedAt time.Time
	if v, exists := fields["reported_at"]; !exists || json.Unmarshal(v, &reportedAt) != nil {
		return nil, "schema_incompatible: missing or invalid reported_at", false
	}

	extra := make(map[string]any)
	known := map[string]bool{
		"report_id":           true,
		"volcano_id":          true,
		"alert_level":         true,
		"eruption_count_24h":  true,
		"ash_column_height_m": true,
		"reported_at":         true,
	}
	for k, v := range fields {
		if known[k] {
			continue
		}
		var val any
		if err := json.Unmarshal(v, &val); err == nil {
			extra[k] = val
		}
	}

	return &VolcanoReport{
		ReportID:         reportID,
		VolcanoID:        volcanoID,
		AlertLevel:       alertLevel,
		EruptionCount24h: int(eruptionCountFloat),
		AshColumnHeightM: ashHeight,
		ReportedAt:       reportedAt,
		Extra:            extra,
	}, "", true
}

func extractSourceRefID(raw []byte) string {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err == nil {
		if id, ok := m["report_id"].(string); ok && strings.TrimSpace(id) != "" {
			return id
		}
	}
	return ""
}

func publishToDLQ(ctx context.Context, source string, raw []byte, reason string, retryable bool, seq int64) {
	encoding := "utf8"
	payloadStr := string(raw)
	if !utf8.Valid(raw) {
		encoding = "base64"
		payloadStr = base64.StdEncoding.EncodeToString(raw)
	}
	status := "parked"
	if retryable {
		status = "retrying"
	}
	rec := dlqRecord{
		MessageID:   uuid.NewString(),
		Source:      source,
		SourceRefID: extractSourceRefID(raw),
		RawPayload:  payloadStr,
		RawEncoding: encoding,
		ReceivedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		Reason:      reason,
		Retryable:   retryable,
		Attempts:    1,
		Status:      status,
		Seq:         seq,
	}
	data, err := json.Marshal(rec)
	if err != nil {
		slog.ErrorContext(ctx, "failed to marshal DLQ record", "err", err)
		return
	}

	topic := envOrDefault("KAFKA_TOPIC_HAZARD_DLQ", "hazard-dlq")
	writer := &kafka.Writer{
		Addr:     kafka.TCP(envOrDefault("KAFKA_BROKERS", "kafka-aggregate:29092")),
		Topic:    topic,
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()

	key := rec.SourceRefID
	if key == "" {
		key = rec.MessageID
	}
	if err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: data,
	}); err != nil {
		slog.ErrorContext(ctx, "failed to publish record to DLQ", "topic", topic, "reason", reason, "err", err)
		return
	}
	slog.WarnContext(ctx, "record sent to DLQ", "topic", topic, "source", source, "source_ref_id", rec.SourceRefID, "reason", reason, "retryable", retryable, "status", status)
}
