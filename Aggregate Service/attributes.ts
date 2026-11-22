/**
 * Converts plain JS values into protobuf Value/Struct shapes for gRPC responses,
 * and unwraps protobuf Struct shapes (including legacy double-wrapped Mongo docs)
 * back into plain JS objects before storing or re-encoding.
 */
export function toProtoValue(v: unknown): any {
  if (v === null || v === undefined) return { nullValue: "NULL_VALUE" };
  if (v instanceof Date) return { stringValue: v.toISOString() };
  if (typeof v === "number") return { numberValue: v };
  if (typeof v === "string") return { stringValue: v };
  if (typeof v === "boolean") return { boolValue: v };
  if (Array.isArray(v)) return { listValue: { values: v.map(toProtoValue) } };
  if (typeof v === "object") return { structValue: toStruct(v as Record<string, unknown>) };
  return { stringValue: String(v) };
}

export function toStruct(obj: Record<string, unknown> | undefined): any {
  return {
    fields: Object.fromEntries(
      Object.entries(obj ?? {}).map(([k, v]) => [k, toProtoValue(v)]),
    ),
  };
}

export function fromProtoValue(v: any): unknown {
  if (v === null || v === undefined || typeof v !== "object") return v;
  if ("nullValue" in v || v.kind === "nullValue") return null;
  if ("numberValue" in v || v.kind === "numberValue") return v.numberValue;
  if ("stringValue" in v || v.kind === "stringValue") return v.stringValue;
  if ("boolValue" in v || v.kind === "boolValue") return v.boolValue;
  if ("structValue" in v || v.kind === "structValue") return fromStruct(v.structValue);
  if ("listValue" in v || v.kind === "listValue") return (v.listValue?.values ?? []).map(fromProtoValue);
  return v;
}

export function fromStruct(s: any): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(s?.fields ?? {}).map(([k, v]) => [k, fromProtoValue(v)]),
  );
}

function isProtoStruct(v: unknown): v is { fields: Record<string, unknown> } {
  if (!v || typeof v !== "object" || Array.isArray(v)) return false;
  const keys = Object.keys(v as Record<string, unknown>);
  if (keys.length !== 1 || keys[0] !== "fields") return false;
  const fields = (v as Record<string, unknown>).fields;
  return Boolean(fields) && typeof fields === "object" && !Array.isArray(fields);
}

export function normalizeAttributes(raw: unknown): Record<string, unknown> {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return {};
  let cur: unknown = raw;
  let guard = 0;
  while (isProtoStruct(cur) && guard < 5) {
    cur = fromStruct(cur);
    guard++;
  }
  if (!cur || typeof cur !== "object" || Array.isArray(cur)) return {};
  return cur as Record<string, unknown>;
}
