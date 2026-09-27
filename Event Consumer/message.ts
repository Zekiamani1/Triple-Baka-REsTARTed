export type HazardMessage = Record<string, unknown>;

export function parseHazardMessage(value: Buffer | null): HazardMessage {
  if (!value) {
    throw new Error("Received an empty Kafka message");
  }

  let parsed: unknown;

  try {
    parsed = JSON.parse(value.toString("utf8"));
  } catch {
    throw new Error("Kafka message is not valid JSON");
  }

  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("Kafka message must contain a JSON object");
  }

  return parsed as HazardMessage;
}
