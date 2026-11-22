package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"unicode/utf8"
)

func TestSchemaChangeModes(t *testing.T) {
	t.Cleanup(func() {
		stateMu.Lock()
		schemaMode = "v1"
		stateMu.Unlock()
	})

	modes := []string{"v1", "additive", "v2", "breaking_rename", "breaking_type", "breaking_missing"}
	for _, m := range modes {
		req := httptest.NewRequest(http.MethodPost, "/admin/schema-version", bytes.NewBufferString(`{"mode":"`+m+`"}`))
		rec := httptest.NewRecorder()
		schemachange(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("mode %s returned %d", m, rec.Code)
		}
	}

	// Empty body defaults to additive (M1 compatibility)
	req := httptest.NewRequest(http.MethodPost, "/admin/schema-version", nil)
	rec := httptest.NewRecorder()
	schemachange(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("empty body returned %d", rec.Code)
	}
	stateMu.RLock()
	got := schemaMode
	stateMu.RUnlock()
	if got != "additive" {
		t.Fatalf("expected additive, got %s", got)
	}

	// Invalid mode returns 400
	reqBad := httptest.NewRequest(http.MethodPost, "/admin/schema-version", bytes.NewBufferString(`{"mode":"unknown"}`))
	recBad := httptest.NewRecorder()
	schemachange(recBad, reqBad)
	if recBad.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recBad.Code)
	}
}

func TestFormatReportForMode(t *testing.T) {
	rep := VolcanoReport{
		ReportID:         "r-1",
		VolcanoID:        "Merapi",
		AlertLevel:       "Siaga",
		EruptionCount24h: 4,
		AshColumnHeightM: 1200,
		ReportedAt:       time.Now().UTC(),
	}

	renamed := formatReportForMode(rep, "breaking_rename")
	if _, ok := renamed["alert_level"]; ok {
		t.Fatal("breaking_rename should remove alert_level")
	}
	if renamed["status_tingkat"] != "Siaga" || renamed["mountain_name"] != "Merapi" {
		t.Fatalf("unexpected breaking_rename output: %#v", renamed)
	}

	typed := formatReportForMode(rep, "breaking_type")
	if _, ok := typed["eruption_count_24h"].(string); !ok {
		t.Fatal("breaking_type should make eruption_count_24h a string")
	}

	missing := formatReportForMode(rep, "breaking_missing")
	if _, ok := missing["volcano_id"]; ok {
		t.Fatal("breaking_missing should remove volcano_id")
	}
}

func TestInjectMalformedAndPayloads(t *testing.T) {
	t.Cleanup(func() {
		stateMu.Lock()
		pendingMalformed = nil
		stateMu.Unlock()
	})

	for _, kind := range []string{"truncated", "deep_nesting", "invalid_utf8", "oversized"} {
		req := httptest.NewRequest(http.MethodPost, "/admin/inject-malformed", bytes.NewBufferString(`{"type":"`+kind+`"}`))
		rec := httptest.NewRecorder()
		injectMalformed(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("inject %s returned %d", kind, rec.Code)
		}
	}

	valid := []map[string]any{{"report_id": "ok-1", "volcano_id": "Merapi", "alert_level": "Normal"}}

	if utf8.Valid(buildMalformedPayload("invalid_utf8", valid)) {
		t.Fatal("invalid_utf8 payload should not be valid UTF-8")
	}
	if len(buildMalformedPayload("oversized", valid)) <= 2*1024*1024 {
		t.Fatal("oversized payload should be > 2 MiB")
	}
	var parsed []any
	if err := json.Unmarshal(buildMalformedPayload("truncated", valid), &parsed); err == nil {
		t.Fatal("truncated payload should fail JSON unmarshal")
	}
	if err := json.Unmarshal(buildMalformedPayload("deep_nesting", valid), &parsed); err != nil || len(parsed) != 2 {
		t.Fatalf("deep_nesting should be valid JSON array with 2 items, err=%v", err)
	}
}
