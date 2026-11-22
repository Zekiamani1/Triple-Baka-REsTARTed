package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidatePayloadBytes(t *testing.T) {
	if reason, ok := validatePayloadBytes([]byte(`{"ok":true}`), false); !ok {
		t.Fatalf("expected valid payload, got %s", reason)
	}
	if _, ok := validatePayloadBytes([]byte("abc"), true); ok {
		t.Fatal("expected oversized payload to fail")
	}
	if _, ok := validatePayloadBytes([]byte{0xff, 0xfe, 0xfd}, false); ok {
		t.Fatal("expected invalid UTF-8 to fail")
	}
	deep := strings.Repeat(`{"a":`, 40) + `1` + strings.Repeat(`}`, 40)
	if _, ok := validatePayloadBytes([]byte(deep), false); ok {
		t.Fatal("expected deep nesting > 32 to fail")
	}
}

func TestValidateVolcanoReportSchemaPolicy(t *testing.T) {
	// Valid + additive field
	valid := json.RawMessage(`{
		"report_id": "rep-1",
		"volcano_id": "Merapi",
		"alert_level": "Siaga",
		"eruption_count_24h": 5,
		"ash_column_height_m": 1500.5,
		"reported_at": "2026-11-21T10:00:00Z",
		"confidence_level": 0.92
	}`)
	rep, reason, ok := validateVolcanoReport(valid)
	if !ok || rep == nil {
		t.Fatalf("expected valid additive report, got %s", reason)
	}
	if rep.Extra["confidence_level"] != 0.92 {
		t.Fatalf("expected confidence_level in Extra, got %#v", rep.Extra)
	}

	// Breaking rename
	renamed := json.RawMessage(`{
		"report_id": "rep-2",
		"mountain_name": "Merapi",
		"status_tingkat": "Siaga",
		"eruption_count_24h": 5,
		"ash_column_height_m": 1500.5,
		"reported_at": "2026-11-21T10:00:00Z"
	}`)
	if _, reason, ok := validateVolcanoReport(renamed); ok || !strings.Contains(reason, "schema_incompatible") {
		t.Fatalf("expected schema_incompatible for renamed fields, got ok=%v reason=%s", ok, reason)
	}

	// Breaking type
	typed := json.RawMessage(`{
		"report_id": "rep-3",
		"volcano_id": "Merapi",
		"alert_level": 3,
		"eruption_count_24h": "25 kali",
		"ash_column_height_m": 1500.5,
		"reported_at": "2026-11-21T10:00:00Z"
	}`)
	if _, reason, ok := validateVolcanoReport(typed); ok || !strings.Contains(reason, "schema_incompatible") {
		t.Fatalf("expected schema_incompatible for type change, got ok=%v reason=%s", ok, reason)
	}

	// Breaking missing
	missing := json.RawMessage(`{
		"report_id": "rep-4",
		"alert_level": "Siaga",
		"eruption_count_24h": 5,
		"ash_column_height_m": 1500.5,
		"reported_at": "2026-11-21T10:00:00Z"
	}`)
	if _, reason, ok := validateVolcanoReport(missing); ok || !strings.Contains(reason, "schema_incompatible") {
		t.Fatalf("expected schema_incompatible for missing volcano_id, got ok=%v reason=%s", ok, reason)
	}
}
