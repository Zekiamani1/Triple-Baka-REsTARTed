export type HazardMessage = Record<string, unknown>;

const MAX_PAYLOAD_BYTES = 1048576;
const MAX_JSON_DEPTH = 32;

export class PoisonPillError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PoisonPillError";
  }
}

function getJsonMaxDepth(text: string): number {
  let depth = 0;
  let maxDepth = 0;
  let inString = false;
  let escaped = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (inString) {
      if (escaped) { escaped = false; continue; }
      if (ch === "\\") { escaped = true; continue; }
      if (ch === '"') { inString = false; }
      continue;
    }
    if (ch === '"') { inString = true; }
    else if (ch === "{" || ch === "[") { depth++; if (depth > maxDepth) maxDepth = depth; }
    else if (ch === "}" || ch === "]") { if (depth > 0) depth--; }
  }
  return maxDepth;
}

export function parseHazardMessage(value: Buffer | null): HazardMessage {
  if (!value || value.length === 0) throw new PoisonPillError("Empty Kafka message");
  if (value.length > MAX_PAYLOAD_BYTES) throw new PoisonPillError("Payload exceeds 1 MiB");
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(value);
  } catch {
    throw new PoisonPillError("Invalid UTF-8 in Kafka message");
  }
  if (getJsonMaxDepth(text) > MAX_JSON_DEPTH) {
    throw new PoisonPillError("JSON nesting depth exceeds 32");
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (err) {
    throw new PoisonPillError(`Invalid JSON: ${err instanceof Error ? err.message : String(err)}`);
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new PoisonPillError("Kafka message must be a JSON object");
  }
  return parsed as HazardMessage;
}
