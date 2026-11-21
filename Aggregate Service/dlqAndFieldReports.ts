import { createHash, randomUUID } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { Collection, Db, MongoClient } from "mongodb";
import { handleAuditRoute } from "./auditServer";
import {
  MAX_PAYLOAD_BYTES,
  PoisonPillError,
  validateAndDecodeJsonPayload,
} from "./poisonGuard";
import { logError, logInfo, logWarn } from "./telemetry";

export interface UnprocessedMessageDoc {
  message_id: string;
  source: string;
  source_ref_id: string;
  raw_payload: string;
  raw_encoding: "utf8" | "base64";
  received_at: Date;
  reason: string;
  retryable: boolean;
  attempts: number;
  status: "retrying" | "parked" | "recovered";
  seq: number;
  created_at: Date;
  updated_at: Date;
  recovered_at: Date | null;
}

const VALID_HAZARD_TYPES = new Set(["SEISMIC", "VOLCANIC", "WILDFIRE", "COASTAL"]);
const VALID_SEVERITIES = new Set(["NORMAL", "WASPADA", "SIAGA", "AWAS"]);

const VOLCANO_COORDS: Record<string, { area_name: string; latitude: number; longitude: number }> = {
  Sumbing: { area_name: "Gunung Sumbing", latitude: -7.384, longitude: 110.072 },
  Salak: { area_name: "Gunung Salak", latitude: -6.719, longitude: 106.733 },
  Gede: { area_name: "Gunung Gede", latitude: -6.77, longitude: 106.999 },
  "Tangkuban Parahu": { area_name: "Gunung Tangkuban Parahu", latitude: -6.759, longitude: 107.609 },
  Guntur: { area_name: "Gunung Guntur", latitude: -7.143, longitude: 107.84 },
  Papandayan: { area_name: "Gunung Papandayan", latitude: -7.319, longitude: 107.73 },
  Galunggung: { area_name: "Gunung Galunggung", latitude: -7.256, longitude: 108.078 },
  Ciremai: { area_name: "Gunung Ciremai", latitude: -6.892, longitude: 108.408 },
  Slamet: { area_name: "Gunung Slamet", latitude: -7.242, longitude: 109.208 },
  Dieng: { area_name: "Kompleks Vulkanik Dieng", latitude: -7.2, longitude: 109.92 },
  Sundoro: { area_name: "Gunung Sundoro", latitude: -7.3, longitude: 109.992 },
  Merapi: { area_name: "Gunung Merapi", latitude: -7.54, longitude: 110.446 },
  Kelud: { area_name: "Gunung Kelud", latitude: -7.935, longitude: 112.314 },
  "Arjuno Welirang": { area_name: "Kompleks Arjuno-Welirang", latitude: -7.725, longitude: 112.58 },
  Bromo: { area_name: "Gunung Bromo", latitude: -7.942, longitude: 112.953 },
  Semeru: { area_name: "Gunung Semeru", latitude: -8.108, longitude: 112.922 },
  Lamongan: { area_name: "Gunung Lamongan", latitude: -7.979, longitude: 113.341 },
  Raung: { area_name: "Gunung Raung", latitude: -8.125, longitude: 114.042 },
  Ijen: { area_name: "Kompleks Ijen", latitude: -8.058, longitude: 114.242 },
};

export function hasScope(scopeHeader: string | string[] | undefined, required: string): boolean {
  const raw = Array.isArray(scopeHeader) ? scopeHeader.join(" ") : (scopeHeader ?? "");
  return raw.split(/\s+/).filter(Boolean).includes(required);
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}

function sendError(res: ServerResponse, status: number, code: string, message: string): void {
  sendJson(res, status, { error: { code, message } });
}

async function readRequestBody(req: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of req) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    total += buf.length;
    if (total > MAX_PAYLOAD_BYTES) {
      throw new PoisonPillError("malformed_payload: payload exceeds 1 MiB");
    }
    chunks.push(buf);
  }
  return Buffer.concat(chunks);
}

export function buildFieldReportEvent(
  payload: Record<string, unknown>,
  idempotencyKey?: string,
): Record<string, unknown> {
  const reportId =
    (typeof payload.report_id === "string" && payload.report_id.trim()) ||
    (typeof idempotencyKey === "string" && idempotencyKey.trim()) ||
    "";
  if (!reportId) {
    throw new Error("report_id or Idempotency-Key is required");
  }

  const hazardType = String(payload.hazard_type ?? "").toUpperCase();
  if (!VALID_HAZARD_TYPES.has(hazardType)) {
    throw new Error("hazard_type must be one of SEISMIC, VOLCANIC, WILDFIRE, COASTAL");
  }

  const severity = String(payload.severity ?? "").toUpperCase();
  if (!VALID_SEVERITIES.has(severity)) {
    throw new Error("severity must be one of NORMAL, WASPADA, SIAGA, AWAS");
  }

  const areaName = typeof payload.area_name === "string" ? payload.area_name.trim() : "";
  if (!areaName) {
    throw new Error("area_name is required");
  }

  const latitude = Number(payload.latitude);
  const longitude = Number(payload.longitude);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    throw new Error("latitude and longitude must be finite numbers");
  }

  const occurredRaw = payload.occurred_at ?? new Date().toISOString();
  const occurredDate = new Date(String(occurredRaw));
  if (Number.isNaN(occurredDate.getTime())) {
    throw new Error("occurred_at must be a valid ISO 8601 timestamp");
  }

  const attributes =
    payload.attributes && typeof payload.attributes === "object" && !Array.isArray(payload.attributes)
      ? (payload.attributes as Record<string, unknown>)
      : {};

  const deterministicHazardId = createHash("sha256")
    .update(`TIM_LAPANGAN:${reportId}`)
    .digest("hex")
    .slice(0, 32);

  return {
    hazard_id: deterministicHazardId,
    source: "TIM_LAPANGAN",
    source_ref_id: reportId,
    hazard_type: hazardType,
    severity,
    area_name: areaName,
    latitude,
    longitude,
    occurred_at: occurredDate.toISOString(),
    ingested_at: new Date().toISOString(),
    attributes,
  };
}

function mapAlertLevelToSeverity(val: unknown): string {
  if (typeof val === "number") {
    const byNum: Record<number, string> = { 1: "NORMAL", 2: "WASPADA", 3: "SIAGA", 4: "AWAS" };
    return byNum[val] ?? "NORMAL";
  }
  const normalized = String(val ?? "").trim().toUpperCase();
  if (VALID_SEVERITIES.has(normalized)) return normalized;
  return "NORMAL";
}

export function adaptRecordForReplay(doc: UnprocessedMessageDoc): Record<string, unknown> {
  if (doc.raw_encoding === "base64") {
    throw new Error("cannot replay binary/non-utf8 payload without manual decoding");
  }

  const parsed = JSON.parse(doc.raw_payload) as Record<string, unknown>;
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("raw_payload is not a JSON object");
  }

  // Already canonical HazardEvent (e.g. from hazard-ingest or field-reports-ingest)
  if (typeof parsed.hazard_id === "string" && typeof parsed.source === "string" && typeof parsed.hazard_type === "string") {
    return parsed;
  }

  // Apply configurable field aliases (PVMBG_SCHEMA_ALIASES) + built-in breaking schema rules
  let aliases: Record<string, string> = {
    status_tingkat: "alert_level",
    mountain_name: "volcano_id",
  };
  if (process.env.PVMBG_SCHEMA_ALIASES) {
    try {
      aliases = { ...aliases, ...JSON.parse(process.env.PVMBG_SCHEMA_ALIASES) };
    } catch {
      // ignore invalid JSON env override
    }
  }

  const adapted: Record<string, unknown> = { ...parsed };
  for (const [oldKey, newKey] of Object.entries(aliases)) {
    if (adapted[oldKey] !== undefined && adapted[newKey] === undefined) {
      adapted[newKey] = adapted[oldKey];
      delete adapted[oldKey];
    }
  }

  const reportId = String(adapted.report_id ?? doc.source_ref_id ?? doc.message_id);
  const volcanoId = String(adapted.volcano_id ?? "Unknown");
  const ref = VOLCANO_COORDS[volcanoId] ?? { area_name: volcanoId === "Unknown" ? "Unknown Volcano" : `Gunung ${volcanoId}`, latitude: 0, longitude: 0 };

  let eruptionCount = 0;
  if (typeof adapted.eruption_count_24h === "number") {
    eruptionCount = adapted.eruption_count_24h;
  } else if (typeof adapted.eruption_count_24h === "string") {
    const parsedNum = Number.parseInt(adapted.eruption_count_24h, 10);
    if (!Number.isNaN(parsedNum)) eruptionCount = parsedNum;
  }

  const ashHeight = Number(adapted.ash_column_height_m ?? 0);
  const occurredAt = adapted.reported_at ? new Date(String(adapted.reported_at)) : doc.received_at;

  const attributes: Record<string, unknown> = {
    eruption_count_24h: eruptionCount,
    ash_column_height_m: Number.isFinite(ashHeight) ? ashHeight : 0,
  };
  for (const [k, v] of Object.entries(adapted)) {
    if (!["report_id", "volcano_id", "alert_level", "eruption_count_24h", "ash_column_height_m", "reported_at"].includes(k)) {
      attributes[k] = v;
    }
  }

  return {
    hazard_id: createHash("sha256").update(`${doc.source}:${reportId}`).digest("hex").slice(0, 32),
    source: doc.source || "PVMBG",
    source_ref_id: reportId,
    hazard_type: "VOLCANIC",
    severity: mapAlertLevelToSeverity(adapted.alert_level),
    area_name: ref.area_name,
    latitude: ref.latitude,
    longitude: ref.longitude,
    occurred_at: Number.isNaN(occurredAt.getTime()) ? doc.received_at.toISOString() : occurredAt.toISOString(),
    ingested_at: new Date().toISOString(),
    attributes,
  };
}

export async function ensureUnprocessedIndexes(db: Db): Promise<Collection<UnprocessedMessageDoc>> {
  const col = db.collection<UnprocessedMessageDoc>("unprocessed_messages");
  await col.createIndex({ message_id: 1 }, { unique: true });
  await col.createIndex({ status: 1, received_at: 1 });
  await col.createIndex({ source: 1, source_ref_id: 1, seq: 1 });
  return col;
}

export async function parkUnprocessedMessage(
  col: Collection<UnprocessedMessageDoc>,
  input: {
    message_id?: string;
    source: string;
    source_ref_id?: string;
    raw_payload: string;
    raw_encoding?: "utf8" | "base64";
    received_at?: Date | string;
    reason: string;
    retryable: boolean;
    attempts?: number;
    status?: "retrying" | "parked" | "recovered";
    seq?: number;
  },
): Promise<UnprocessedMessageDoc> {
  const now = new Date();
  const receivedAt = input.received_at ? new Date(input.received_at) : now;
  const status = input.status ?? (input.retryable ? "retrying" : "parked");
  const doc: UnprocessedMessageDoc = {
    message_id: input.message_id || randomUUID(),
    source: input.source || "UNKNOWN",
    source_ref_id: input.source_ref_id ?? "",
    raw_payload: input.raw_payload,
    raw_encoding: input.raw_encoding ?? "utf8",
    received_at: Number.isNaN(receivedAt.getTime()) ? now : receivedAt,
    reason: input.reason,
    retryable: input.retryable,
    attempts: input.attempts ?? 1,
    status,
    seq: input.seq ?? 0,
    created_at: now,
    updated_at: now,
    recovered_at: null,
  };

  await col.updateOne(
    { message_id: doc.message_id },
    {
      $set: {
        reason: doc.reason,
        retryable: doc.retryable,
        status: doc.status,
        updated_at: now,
      },
      $setOnInsert: {
        source: doc.source,
        source_ref_id: doc.source_ref_id,
        raw_payload: doc.raw_payload,
        raw_encoding: doc.raw_encoding,
        received_at: doc.received_at,
        seq: doc.seq,
        created_at: doc.created_at,
        recovered_at: null,
      },
      $max: { attempts: doc.attempts },
    },
    { upsert: true },
  );

  return doc;
}

export function startInternalHttpServer(deps: {
  mongoClient: MongoClient;
  dbName: string;
  collectionName: string;
  unprocessedCol: Collection<UnprocessedMessageDoc>;
  ingestHazard: (event: Record<string, unknown>, opts?: { upsert?: boolean }) => Promise<void>;
  publishFieldReport: (event: Record<string, unknown>) => Promise<void>;
}): Server {
  const port = Number(process.env.AUDIT_HTTP_PORT ?? "50052");

  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    const method = req.method ?? "GET";

    try {
      if (
        await handleAuditRoute(req, res, url, {
          mongoClient: deps.mongoClient,
          dbName: deps.dbName,
          collectionName: deps.collectionName,
          unprocessedCol: deps.unprocessedCol,
        })
      ) {
        return;
      }
      if (method === "GET" && url.pathname === "/health") {
        res.writeHead(200);
        res.end("UP");
        return;
      }

      if (method === "POST" && url.pathname === "/field-reports") {
        if (!hasScope(req.headers["x-client-scope"], "field_report:write")) {
          sendError(res, 403, "forbidden", "missing required scope: field_report:write");
          return;
        }
        let bodyBuf: Buffer;
        try {
          bodyBuf = await readRequestBody(req);
        } catch (err) {
          sendError(res, 400, "bad_request", err instanceof Error ? err.message : "invalid payload");
          return;
        }
        let payload: Record<string, unknown>;
        try {
          payload = validateAndDecodeJsonPayload(bodyBuf);
        } catch (err) {
          sendError(res, 400, "bad_request", err instanceof Error ? err.message : "invalid JSON");
          return;
        }
        let event: Record<string, unknown>;
        try {
          const idemKey = Array.isArray(req.headers["idempotency-key"])
            ? req.headers["idempotency-key"][0]
            : req.headers["idempotency-key"];
          event = buildFieldReportEvent(payload, idemKey);
        } catch (err) {
          sendError(res, 400, "bad_request", err instanceof Error ? err.message : "validation failed");
          return;
        }

        await deps.publishFieldReport(event);
        const correlationId =
          (Array.isArray(req.headers["x-correlation-id"])
            ? req.headers["x-correlation-id"][0]
            : req.headers["x-correlation-id"]) ?? randomUUID();

        sendJson(res, 202, {
          status: "accepted",
          report_id: event.source_ref_id,
          hazard_id: event.hazard_id,
          correlation_id: correlationId,
        });
        return;
      }

      if (method === "GET" && url.pathname === "/internal/dlq/messages") {
        if (!hasScope(req.headers["x-client-scope"], "dlq:read")) {
          sendError(res, 403, "forbidden", "missing required scope: dlq:read");
          return;
        }
        const statusParam = url.searchParams.get("status");
        const sourceParam = url.searchParams.get("source");
        const limit = Math.min(Math.max(Number(url.searchParams.get("limit") ?? "100") || 100, 1), 1000);
        const filter: Record<string, unknown> = {};
        if (statusParam) filter.status = statusParam;
        if (sourceParam) filter.source = sourceParam;

        const items = await deps.unprocessedCol
          .find(filter, { projection: { _id: 0 } })
          .sort({ received_at: 1, seq: 1, message_id: 1 })
          .limit(limit)
          .toArray();

        sendJson(res, 200, { items, total: items.length });
        return;
      }

      if (method === "POST" && url.pathname === "/internal/dlq/replay") {
        if (!hasScope(req.headers["x-client-scope"], "dlq:replay")) {
          sendError(res, 403, "forbidden", "missing required scope: dlq:replay");
          return;
        }
        const bodyBuf = await readRequestBody(req);
        let filterBody: { source?: string; source_ref_id?: string; message_ids?: string[] } = {};
        if (bodyBuf.length > 0) {
          filterBody = validateAndDecodeJsonPayload(bodyBuf) as typeof filterBody;
        }

        const query: Record<string, unknown> = { status: "parked" };
        if (filterBody.source) query.source = filterBody.source;
        if (filterBody.source_ref_id) query.source_ref_id = filterBody.source_ref_id;
        if (Array.isArray(filterBody.message_ids) && filterBody.message_ids.length > 0) {
          query.message_id = { $in: filterBody.message_ids };
        }

        // Order strictly by source_ref_id, received_at ASC, seq ASC
        const parked = await deps.unprocessedCol
          .find(query)
          .sort({ source_ref_id: 1, received_at: 1, seq: 1 })
          .toArray();

        let replayed = 0;
        let failed = 0;
        const results: Array<{ message_id: string; source_ref_id: string; status: string; error?: string }> = [];

        for (const doc of parked) {
          try {
            const event = adaptRecordForReplay(doc);
            await deps.ingestHazard(event, { upsert: true });
            const now = new Date();
            await deps.unprocessedCol.updateOne(
              { message_id: doc.message_id },
              {
                $set: { status: "recovered", recovered_at: now, updated_at: now },
                $inc: { attempts: 1 },
              },
            );
            replayed++;
            results.push({ message_id: doc.message_id, source_ref_id: doc.source_ref_id, status: "recovered" });
          } catch (err) {
            failed++;
            const msg = err instanceof Error ? err.message : String(err);
            logWarn("DLQ replay failed for message", { message_id: doc.message_id, err: msg });
            results.push({ message_id: doc.message_id, source_ref_id: doc.source_ref_id, status: "parked", error: msg });
          }
        }

        sendJson(res, 200, { replayed, failed, results });
        return;
      }

      sendError(res, 404, "not_found", "endpoint not found");
    } catch (err) {
      logError("internal HTTP server error", {
        path: url.pathname,
        err: err instanceof Error ? err.message : String(err),
      });
      sendError(res, 500, "internal_error", "internal server error");
    }
  });

  server.listen(port, "0.0.0.0", () => {
    logInfo("Aggregate internal HTTP server started", { port });
  });

  return server;
}
