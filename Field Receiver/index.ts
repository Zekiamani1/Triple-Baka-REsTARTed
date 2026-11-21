import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { timingSafeEqual } from "node:crypto";
import { receiverConfig } from "./config";
import { IdempotencyConflictError, ReceiptStore } from "./store";
import { currentTraceFields, shutdownTelemetry } from "./telemetry";

const store = new ReceiptStore();
let server: Server | undefined;
let shuttingDown = false;
let shutdownPromise: Promise<void> | undefined;

interface NotificationRequest {
  delivery_id: string;
  event: Record<string, unknown>;
}

class HttpError extends Error {
  constructor(readonly statusCode: number, message: string) {
    super(message);
    this.name = "HttpError";
  }
}

function log(level: "INFO" | "WARN" | "ERROR", message: string, fields: Record<string, unknown> = {}): void {
  const entry = {
    time: new Date().toISOString(),
    level,
    msg: message,
    ...currentTraceFields(),
    ...fields,
  };
  const line = `${JSON.stringify(entry)}\n`;
  if (level === "ERROR") process.stderr.write(line);
  else process.stdout.write(line);
}

function bearerTokenMatches(header: string | undefined, expected: string): boolean {
  if (!header?.startsWith("Bearer ")) return false;
  const supplied = Buffer.from(header.slice("Bearer ".length), "utf8");
  const reference = Buffer.from(expected, "utf8");
  return supplied.length === reference.length && timingSafeEqual(supplied, reference);
}

async function readJsonBody(request: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  let totalBytes = 0;

  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    totalBytes += buffer.length;
    if (totalBytes > receiverConfig.maxBodyBytes) {
      throw new HttpError(413, "request body too large");
    }
    chunks.push(buffer);
  }

  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;
  } catch {
    throw new HttpError(400, "invalid JSON body");
  }
}

function parseNotificationRequest(value: unknown): NotificationRequest {
  if (!isRecord(value)) throw new HttpError(400, "request body must be a JSON object");
  const deliveryId = typeof value.delivery_id === "string" ? value.delivery_id.trim() : "";
  if (!deliveryId) throw new HttpError(400, "delivery_id is required");
  if (!isRecord(value.event)) throw new HttpError(400, "event must be a JSON object");

  const event = value.event;
  const source = typeof event.source === "string" ? event.source.toUpperCase() : "";
  const hazardType = typeof event.hazard_type === "string" ? event.hazard_type.toUpperCase() : "";
  const critical = readStructField(event.attributes, "critical");

  if (source !== "SENSOR_PANTAI" || hazardType !== "COASTAL" || critical !== true) {
    throw new HttpError(400, "receiver accepts only SENSOR_PANTAI/COASTAL events with critical=true");
  }

  return { delivery_id: deliveryId, event };
}

function readStructField(value: unknown, fieldName: string): unknown {
  if (!isRecord(value)) return undefined;
  if (Object.prototype.hasOwnProperty.call(value, fieldName)) {
    return unwrapProtoValue(value[fieldName]);
  }
  const fields = value.fields;
  if (fields instanceof Map) return unwrapProtoValue(fields.get(fieldName));
  if (isRecord(fields) && Object.prototype.hasOwnProperty.call(fields, fieldName)) {
    return unwrapProtoValue(fields[fieldName]);
  }
  return undefined;
}

function unwrapProtoValue(value: unknown): unknown {
  if (!isRecord(value)) return value;
  const kind = typeof value.kind === "string" ? value.kind : undefined;
  if (kind === "boolValue") return value.boolValue;
  if (kind === "stringValue") return value.stringValue;
  if (kind === "numberValue") return value.numberValue;
  if (kind === "nullValue") return null;
  for (const key of ["boolValue", "stringValue", "numberValue", "nullValue"] as const) {
    if (Object.prototype.hasOwnProperty.call(value, key)) return key === "nullValue" ? null : value[key];
  }
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function sendJson(response: ServerResponse, statusCode: number, body: Record<string, unknown>): void {
  response.writeHead(statusCode, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

async function handleRequest(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);

  if (request.method === "GET" && url.pathname === "/healthz") {
    sendJson(response, 200, { status: "ok" });
    return;
  }
  if (request.method === "GET" && url.pathname === "/readyz") {
    const ready = store.isConnected() && !shuttingDown;
    sendJson(response, ready ? 200 : 503, { status: ready ? "ready" : "not_ready" });
    return;
  }
  if (request.method !== "POST" || url.pathname !== "/notifications") {
    sendJson(response, 404, { error: "not_found" });
    return;
  }

  if (!bearerTokenMatches(request.headers.authorization, receiverConfig.token)) {
    sendJson(response, 401, { error: "unauthorized" });
    return;
  }

  const parsed = parseNotificationRequest(await readJsonBody(request));
  const suppliedIdempotencyKey = request.headers["idempotency-key"];
  if (typeof suppliedIdempotencyKey === "string" && suppliedIdempotencyKey !== parsed.delivery_id) {
    throw new HttpError(400, "Idempotency-Key must match delivery_id");
  }

  const receipt = await store.saveReceipt(parsed.delivery_id, parsed.event);
  log("INFO", receipt.duplicate ? "duplicate notification receipt acknowledged" : "notification durably received", {
    delivery_id: receipt.delivery_id,
    receipt_id: receipt.receipt_id,
    duplicate: receipt.duplicate,
  });
  sendJson(response, receipt.duplicate ? 200 : 201, receipt);
}

async function start(): Promise<void> {
  await store.connect();

  server = createServer((request, response) => {
    void handleRequest(request, response).catch((error: unknown) => {
      if (error instanceof HttpError) {
        sendJson(response, error.statusCode, { error: error.message });
        return;
      }
      if (error instanceof IdempotencyConflictError) {
        sendJson(response, 409, { error: "idempotency_conflict", message: error.message });
        return;
      }
      log("ERROR", "notification request failed before durable acknowledgment", {
        err: error instanceof Error ? error.message : String(error),
      });
      if (!response.headersSent) sendJson(response, 503, { error: "receipt_storage_unavailable" });
      else response.destroy();
    });
  });

  await new Promise<void>((resolve, reject) => {
    server!.once("error", reject);
    server!.listen(receiverConfig.port, () => {
      server!.removeListener("error", reject);
      resolve();
    });
  });

  log("INFO", "Field Receiver started", { port: receiverConfig.port });
}

async function shutdown(): Promise<void> {
  if (shutdownPromise) return shutdownPromise;
  shuttingDown = true;
  shutdownPromise = (async () => {
    if (server) {
      await new Promise<void>((resolve) => server!.close(() => resolve()));
      server = undefined;
    }
    await store.close().catch((error) => {
      log("ERROR", "failed to close receipt store", { err: String(error) });
    });
    await shutdownTelemetry().catch((error) => {
      log("ERROR", "failed to shutdown telemetry", { err: String(error) });
    });
  })();
  return shutdownPromise;
}

process.once("SIGINT", () => void shutdown().finally(() => { process.exitCode = 0; }));
process.once("SIGTERM", () => void shutdown().finally(() => { process.exitCode = 0; }));

start().catch(async (error) => {
  log("ERROR", "Field Receiver startup failed", { err: error instanceof Error ? error.message : String(error) });
  await shutdown();
  process.exitCode = 1;
});
