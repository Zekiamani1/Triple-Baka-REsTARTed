function required(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;
  if (!value || value.trim() === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value.trim();
}

function positiveInteger(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

const generalGroupId = required("KAFKA_CONSUMER_GROUP", "emergency-notifier");

export const kafkaConfig = {
  brokers: required("KAFKA_BROKERS", "localhost:9092")
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean),
  clientId: required("KAFKA_CLIENT_ID", "alert-consumer"),
  groupId: generalGroupId,
  criticalGroupId: required("KAFKA_CRITICAL_CONSUMER_GROUP", `${generalGroupId}-critical`),
  topic: required("KAFKA_TOPIC_HAZARD_EVENTS", "hazard-events"),
  criticalTopic: required("KAFKA_TOPIC_HAZARD_CRITICAL", "hazard-critical"),
  fromBeginning: (process.env.KAFKA_FROM_BEGINNING ?? "true").toLowerCase() === "true",
};

export const receiverConfig = {
  url: required("FIELD_RECEIVER_URL", "http://field-receiver:8090/notifications"),
  token: required("FIELD_RECEIVER_TOKEN"),
  timeoutMs: positiveInteger("DELIVERY_TIMEOUT_MS", 2000),
  maxAttempts: positiveInteger("DELIVERY_MAX_ATTEMPTS", 5),
  baseBackoffMs: positiveInteger("DELIVERY_BASE_BACKOFF_MS", 250),
  maxBackoffMs: positiveInteger("DELIVERY_MAX_BACKOFF_MS", 5000),
};

export const healthPort = positiveInteger("HEALTH_PORT", 8088);
