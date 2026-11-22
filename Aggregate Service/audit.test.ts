import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import {
  computeByteOrderChecksum,
  decodeCursor,
  encodeCursor,
  unwrapAttributes,
  verifyAuditBearerToken,
} from "./auditServer";

test("verifyAuditBearerToken uses constant-time comparison", () => {
  assert.equal(verifyAuditBearerToken("Bearer secret-123", "secret-123"), true);
  assert.equal(verifyAuditBearerToken("Bearer secret-124", "secret-123"), false);
  assert.equal(verifyAuditBearerToken("secret-123", "secret-123"), false);
  assert.equal(verifyAuditBearerToken(undefined, "secret-123"), false);
});

test("computeByteOrderChecksum sorts UTF-8 bytes and handles empty list", () => {
  assert.equal(
    computeByteOrderChecksum([]),
    createHash("sha256").update("").digest("hex"),
  );
  // "B" (0x42) < "a" (0x61) in UTF-8 byte order
  const expected = createHash("sha256").update("B\na", "utf8").digest("hex");
  assert.equal(computeByteOrderChecksum(["a", "B"]), expected);
});

test("encodeCursor and decodeCursor round-trip", () => {
  const now = new Date("2026-11-21T10:00:00.000Z");
  const cur = encodeCursor(now, "haz-1");
  const decoded = decodeCursor(cur);
  assert.ok(decoded);
  assert.equal(decoded.t.toISOString(), now.toISOString());
  assert.equal(decoded.id, "haz-1");
  assert.equal(decodeCursor("not-valid-base64!"), null);
});

test("unwrapAttributes converts protobuf Struct fields to plain object", () => {
  const struct = {
    fields: {
      magnitude: { numberValue: 5.4 },
      tsunami: { boolValue: true },
      note: { stringValue: "ok" },
    },
  };
  assert.deepEqual(unwrapAttributes(struct), {
    magnitude: 5.4,
    tsunami: true,
    note: "ok",
  });
});
