import assert from "node:assert/strict";
import test from "node:test";
import { parseHazardMessage } from "./message";

test("parses a JSON hazard event", () => {
  const event = parseHazardMessage(
    Buffer.from('{"hazard_id":"hazard-test-001","source":"BMKG"}'),
  );

  assert.deepEqual(event, {
    hazard_id: "hazard-test-001",
    source: "BMKG",
  });
});

test("rejects an empty Kafka message", () => {
  assert.throws(() => parseHazardMessage(null), /empty/i);
});

test("rejects malformed JSON", () => {
  assert.throws(
    () => parseHazardMessage(Buffer.from("not-json")),
    /valid JSON/i,
  );
});
