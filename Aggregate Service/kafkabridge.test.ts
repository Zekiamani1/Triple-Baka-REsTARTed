import assert from "node:assert/strict";
import test from "node:test";
import { createHazardKafkaMessage } from "./kafkabridge";

test("creates a Kafka message keyed by hazard_id", () => {
  const hazard = {
    hazard_id: "hazard-test-001",
    source: "BMKG",
    severity: "SIAGA",
  };

  const message = createHazardKafkaMessage(hazard);

  assert.equal(message.key, "hazard-test-001");
  assert.deepEqual(JSON.parse(String(message.value)), hazard);
});

test("rejects a hazard without hazard_id", () => {
  assert.throws(
    () => createHazardKafkaMessage({ source: "BMKG" }),
    /hazard_id/,
  );
});
