import assert from "node:assert/strict";
import test from "node:test";
import { PoisonPillError, validateAndDecodeJsonPayload } from "./poisonGuard";
import { adaptRecordForReplay, buildFieldReportEvent, hasScope, type UnprocessedMessageDoc } from "./dlqAndFieldReports";

test("PoisonPillGuard rejects oversized, invalid UTF-8, deep nesting, and truncated JSON", () => {
  assert.throws(() => validateAndDecodeJsonPayload(Buffer.alloc(1048577, 97)), PoisonPillError);
  assert.throws(() => validateAndDecodeJsonPayload(Buffer.from([0xff, 0xfe, 0xfd])), PoisonPillError);

  const deep = `${'{"a":'.repeat(40)}1${"}".repeat(40)}`;
  assert.throws(() => validateAndDecodeJsonPayload(Buffer.from(deep)), PoisonPillError);

  assert.throws(() => validateAndDecodeJsonPayload(Buffer.from('{"a":')), PoisonPillError);
  assert.deepEqual(validateAndDecodeJsonPayload(Buffer.from('{"ok":true}')), { ok: true });
});

test("hasScope checks space-delimited tokens", () => {
  assert.equal(hasScope("hazard:read:full field_report:write", "field_report:write"), true);
  assert.equal(hasScope("hazard:read:full field_report:write", "dlq:read"), false);
  assert.equal(hasScope("hazard:read:full dlq:read dlq:replay", "dlq:replay"), true);
});

test("buildFieldReportEvent validates and constructs canonical TIM_LAPANGAN event", () => {
  const ev = buildFieldReportEvent(
    {
      report_id: "rep-field-001",
      hazard_type: "VOLCANIC",
      severity: "SIAGA",
      area_name: "Gunung Merapi",
      latitude: -7.54,
      longitude: 110.446,
      occurred_at: "2026-11-21T10:00:00Z",
      attributes: { reporter: "Tim A" },
    },
  );
  assert.equal(ev.source, "TIM_LAPANGAN");
  assert.equal(ev.source_ref_id, "rep-field-001");
  assert.equal(ev.hazard_type, "VOLCANIC");
  assert.equal(ev.severity, "SIAGA");
});

test("adaptRecordForReplay adapts PVMBG breaking_rename and breaking_type", () => {
  const renamedDoc: UnprocessedMessageDoc = {
    message_id: "m1",
    source: "PVMBG",
    source_ref_id: "r1",
    raw_payload: JSON.stringify({
      report_id: "r1",
      mountain_name: "Merapi",
      status_tingkat: "Awas",
      eruption_count_24h: "25 kali",
      ash_column_height_m: 3000,
      reported_at: "2026-11-21T10:00:00Z",
    }),
    raw_encoding: "utf8",
    received_at: new Date(),
    reason: "schema_incompatible",
    retryable: false,
    attempts: 1,
    status: "parked",
    seq: 0,
    created_at: new Date(),
    updated_at: new Date(),
    recovered_at: null,
  };

  const adapted = adaptRecordForReplay(renamedDoc);
  assert.equal(adapted.source, "PVMBG");
  assert.equal(adapted.source_ref_id, "r1");
  assert.equal(adapted.severity, "AWAS");
  assert.equal(adapted.area_name, "Gunung Merapi");
  assert.equal((adapted.attributes as Record<string, unknown>).eruption_count_24h, 25);
});
