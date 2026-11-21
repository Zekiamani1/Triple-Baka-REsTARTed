export type HazardMessage = Record<string, unknown>;

type UnknownRecord = Record<string, unknown>;

export function parseHazardMessage(value: Buffer | null): HazardMessage {
  if (!value || value.length === 0) throw new Error("Empty Kafka message");
  const parsed: unknown = JSON.parse(value.toString("utf8"));
  if (!isRecord(parsed)) throw new Error("Hazard Kafka payload must be a JSON object");
  return parsed;
}

export function isEmergency(event: HazardMessage): boolean {
  const severity = readString(event.severity)?.toUpperCase();
  return severity === "SIAGA" || severity === "AWAS";
}

export function isCoastalEvent(event: HazardMessage): boolean {
  return readString(event.source)?.toUpperCase() === "SENSOR_PANTAI" &&
    readString(event.hazard_type)?.toUpperCase() === "COASTAL";
}

/**
 * Supports both JSON-mapped google.protobuf.Struct values and decoded
 * protobuf Value objects (e.g. { kind: "boolValue", boolValue: true }).
 */
export function readAttribute(event: HazardMessage, fieldName: string): unknown {
  const attributes = event.attributes;
  if (!isRecord(attributes)) return undefined;

  if (Object.prototype.hasOwnProperty.call(attributes, fieldName)) {
    return unwrapProtoValue(attributes[fieldName]);
  }

  const fields = attributes.fields;
  if (fields instanceof Map) {
    return unwrapProtoValue(fields.get(fieldName));
  }
  if (isRecord(fields) && Object.prototype.hasOwnProperty.call(fields, fieldName)) {
    return unwrapProtoValue(fields[fieldName]);
  }

  return undefined;
}

export function isCriticalCoastal(event: HazardMessage): boolean {
  return isCoastalEvent(event) && readAttribute(event, "critical") === true;
}

/**
 * The delivery key must remain stable across Kafka redelivery and process restarts.
 * Prefer source + source_ref_id; fall back to deterministic hazard_id.
 */
export function getDeliveryId(event: HazardMessage): string {
  const source = readString(event.source)?.toUpperCase();
  const sourceRefId = readString(event.source_ref_id);
  const hazardId = readString(event.hazard_id);

  if (source && sourceRefId) return `${source}:${sourceRefId}`;
  if (hazardId) return hazardId;
  throw new Error("Critical HazardEvent has no stable delivery identity (source_ref_id or hazard_id)");
}

function unwrapProtoValue(value: unknown): unknown {
  if (!isRecord(value)) return value;

  // protobufjs/google-protobuf representations may expose a oneof discriminator.
  const kind = readString(value.kind);
  if (kind) {
    switch (kind) {
      case "boolValue": return value.boolValue;
      case "stringValue": return value.stringValue;
      case "numberValue": return value.numberValue;
      case "nullValue": return null;
      case "structValue": return value.structValue;
      case "listValue": return value.listValue;
      default: break;
    }
  }

  // Some generated representations expose the oneof member without `kind`.
  for (const key of ["boolValue", "stringValue", "numberValue", "nullValue", "structValue", "listValue"] as const) {
    if (Object.prototype.hasOwnProperty.call(value, key)) {
      return key === "nullValue" ? null : value[key];
    }
  }

  return value;
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() !== "" ? value : undefined;
}

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
