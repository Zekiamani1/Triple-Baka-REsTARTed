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

export const receiverConfig = {
  port: positiveInteger("FIELD_RECEIVER_PORT", 8090),
  token: required("FIELD_RECEIVER_TOKEN"),
  mongodbUri: required("FIELD_RECEIVER_MONGODB_URI"),
  database: required("FIELD_RECEIVER_DB", "field_receiver"),
  collection: required("FIELD_RECEIVER_COLLECTION", "notification_receipts"),
  maxBodyBytes: positiveInteger("FIELD_RECEIVER_MAX_BODY_BYTES", 1_048_576),
  serviceName: required("OTEL_SERVICE_NAME", "field-receiver"),
};
