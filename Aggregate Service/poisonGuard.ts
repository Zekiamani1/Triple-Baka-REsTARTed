export const MAX_PAYLOAD_BYTES = 1048576; // 1 MiB
export const MAX_JSON_DEPTH = 32;

export class PoisonPillError extends Error {
  public readonly retryable = false;
  public readonly reason: string;

  constructor(reason: string) {
    super(reason);
    this.name = "PoisonPillError";
    this.reason = reason;
  }
}

export function getJsonMaxDepth(text: string): number {
  let depth = 0;
  let maxDepth = 0;
  let inString = false;
  let escaped = false;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (inString) {
      if (escaped) {
        escaped = false;
        continue;
      }
      if (ch === "\\") {
        escaped = true;
        continue;
      }
      if (ch === '"') {
        inString = false;
      }
      continue;
    }

    if (ch === '"') {
      inString = true;
    } else if (ch === "{" || ch === "[") {
      depth++;
      if (depth > maxDepth) {
        maxDepth = depth;
      }
    } else if (ch === "}" || ch === "]") {
      if (depth > 0) {
        depth--;
      }
    }
  }

  return maxDepth;
}

export function validateAndDecodeJsonPayload(value: Buffer | null | undefined): Record<string, unknown> {
  if (!value || value.length === 0) {
    throw new PoisonPillError("malformed_payload: empty message");
  }
  if (value.length > MAX_PAYLOAD_BYTES) {
    throw new PoisonPillError("malformed_payload: payload exceeds 1 MiB");
  }

  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(value);
  } catch {
    throw new PoisonPillError("malformed_payload: invalid utf-8");
  }

  if (getJsonMaxDepth(text) > MAX_JSON_DEPTH) {
    throw new PoisonPillError(`malformed_payload: json nesting depth exceeds ${MAX_JSON_DEPTH}`);
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (err) {
    throw new PoisonPillError(
      `malformed_payload: invalid JSON (${err instanceof Error ? err.message : String(err)})`,
    );
  }

  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new PoisonPillError("malformed_payload: payload must be a JSON object");
  }

  return parsed as Record<string, unknown>;
}
