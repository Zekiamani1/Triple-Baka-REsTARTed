import { createHash, timingSafeEqual } from "node:crypto";
import { readFileSync } from "node:fs";
import type { IncomingMessage, ServerResponse } from "node:http";
import { MongoClient, type Collection, type Db } from "mongodb";
import { queryWithPartialFallback } from "./partialQuery";
import type { UnprocessedMessageDoc } from "./dlqAndFieldReports";

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}

function sendError(res: ServerResponse, status: number, code: string, message: string): void {
  sendJson(res, status, { error: { code, message } });
}

export function verifyAuditBearerToken(authHeader: string | undefined, expectedToken: string | undefined): boolean {
  if (!expectedToken) return false;
  if (!authHeader || !authHeader.startsWith("Bearer ")) return false;
  const provided = authHeader.slice("Bearer ".length);
  const a = Buffer.from(provided, "utf8");
  const b = Buffer.from(expectedToken, "utf8");
  return a.length === b.length && timingSafeEqual(a, b);
}

export function computeByteOrderChecksum(sourceRefIds: string[]): string {
  const sorted = [...sourceRefIds].sort((a, b) =>
    Buffer.compare(Buffer.from(a, "utf8"), Buffer.from(b, "utf8")),
  );
  return createHash("sha256").update(sorted.join("\n"), "utf8").digest("hex");
}

export function encodeCursor(t: Date | string, id: string): string {
  const iso = t instanceof Date ? t.toISOString() : new Date(t).toISOString();
  return Buffer.from(JSON.stringify({ t: iso, id }), "utf8").toString("base64url");
}

export function decodeCursor(cursor: string): { t: Date; id: string } | null {
  try {
    const raw = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
    if (!raw || typeof raw.t !== "string" || typeof raw.id !== "string") return null;
    const d = new Date(raw.t);
    if (Number.isNaN(d.getTime())) return null;
    return { t: d, id: raw.id };
  } catch {
    return null;
  }
}

function unwrapProtoValue(val: any): unknown {
  if (val === null || val === undefined) return null;
  if (typeof val !== "object") return val;
  if ("stringValue" in val) return val.stringValue;
  if ("numberValue" in val) return val.numberValue;
  if ("boolValue" in val) return val.boolValue;
  if ("nullValue" in val) return null;
  if ("listValue" in val && Array.isArray(val.listValue?.values)) {
    return val.listValue.values.map(unwrapProtoValue);
  }
  if ("structValue" in val) return unwrapAttributes(val.structValue);
  return val;
}

export function unwrapAttributes(attrs: any): Record<string, unknown> {
  if (!attrs || typeof attrs !== "object") return {};
  if (attrs.fields && typeof attrs.fields === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(attrs.fields)) {
      out[k] = unwrapProtoValue(v);
    }
    return out;
  }
  return attrs as Record<string, unknown>;
}

let podNodeCache: { expiresAt: number; map: Map<string, string> } = { expiresAt: 0, map: new Map() };

async function getPodToNodeMap(): Promise<Map<string, string>> {
  const now = Date.now();
  if (now < podNodeCache.expiresAt) return podNodeCache.map;

  const map = new Map<string, string>();
  try {
    const token = readFileSync("/var/run/secrets/kubernetes.io/serviceaccount/token", "utf8").trim();
    const ns = readFileSync("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "utf8").trim();
    const host = process.env.KUBERNETES_SERVICE_HOST;
    const port = process.env.KUBERNETES_SERVICE_PORT ?? "443";
    if (host && token && ns) {
      const resp = await fetch(`https://${host}:${port}/api/v1/namespaces/${ns}/pods`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (resp.ok) {
        const data = (await resp.json()) as { items?: Array<{ metadata?: { name?: string }; spec?: { nodeName?: string } }> };
        for (const item of data.items ?? []) {
          if (item.metadata?.name && item.spec?.nodeName) {
            map.set(item.metadata.name, item.spec.nodeName);
          }
        }
      }
    }
  } catch {
    // Local Docker / non-k8s environment: fallback to hostnames
  }

  podNodeCache = { expiresAt: now + 5000, map };
  return map;
}

export interface ShardLocation {
  node: string;
  unit: string;
  role: "primary" | "replica";
}

export function buildShardLocations(
  shardId: string,
  hosts: string[],
  primaryHost: string | null,
  podToNode: Map<string, string>,
): ShardLocation[] {
  const cleanHosts = hosts.map((h) => h.trim()).filter(Boolean);
  if (cleanHosts.length === 0) return [];

  const norm = (v: string) => v.trim().toLowerCase();
  const primaryNorm = primaryHost ? norm(primaryHost) : null;
  const primaryHostname = primaryNorm ? primaryNorm.split(":")[0] : null;

  let primaryIdx = -1;
  if (primaryNorm) {
    primaryIdx = cleanHosts.findIndex((h) => norm(h) === primaryNorm);
    if (primaryIdx === -1 && primaryHostname) {
      primaryIdx = cleanHosts.findIndex((h) => norm(h).split(":")[0] === primaryHostname);
    }
  }
  if (primaryIdx === -1) primaryIdx = 0;

  const nodeForHost = (h: string): string => {
    const hostOnly = h.split(":")[0];
    const podName = hostOnly.split(".")[0];
    return podToNode.get(podName) ?? hostOnly;
  };

  const primary = cleanHosts[primaryIdx];
  const locations: ShardLocation[] = [
    { node: nodeForHost(primary), unit: shardId, role: "primary" },
  ];
  let replicaNum = 1;
  for (let i = 0; i < cleanHosts.length; i++) {
    if (i === primaryIdx) continue;
    locations.push({
      node: nodeForHost(cleanHosts[i]),
      unit: `${shardId}-r${replicaNum++}`,
      role: "replica",
    });
  }
  return locations;
}

async function resolveShardPrimaryHost(
  mongoClient: MongoClient,
  hostStr: string,
): Promise<string | null> {
  const hostsPart = hostStr.includes("/") ? hostStr.split("/")[1] : hostStr;
  const hosts = hostsPart ? hostsPart.split(",").map((h) => h.trim()).filter(Boolean) : [];
  const credentials = mongoClient.options.credentials;

  for (const host of hosts) {
    const probeClient = new MongoClient(`mongodb://${host}/`, {
      auth: credentials
        ? { username: credentials.username, password: credentials.password }
        : undefined,
      authSource: credentials?.source,
      authMechanism: credentials?.mechanism,
      authMechanismProperties: credentials?.mechanismProperties,
      tls: mongoClient.options.tls,
      tlsCAFile: mongoClient.options.tlsCAFile,
      tlsCertificateKeyFile: mongoClient.options.tlsCertificateKeyFile,
      tlsCertificateKeyFilePassword: (mongoClient.options as { tlsCertificateKeyFilePassword?: string }).tlsCertificateKeyFilePassword,
      tlsAllowInvalidCertificates: mongoClient.options.tlsAllowInvalidCertificates,
      tlsAllowInvalidHostnames: mongoClient.options.tlsAllowInvalidHostnames,
      tlsInsecure: mongoClient.options.tlsInsecure,
      directConnection: true,
      serverSelectionTimeoutMS: 1200,
      connectTimeoutMS: 1200,
      socketTimeoutMS: 1500,
    });
    try {
      let res: Record<string, unknown>;
      try {
        res = await probeClient.db("admin").command({ hello: 1 });
      } catch {
        res = await probeClient.db("admin").command({ isMaster: 1 });
      }
      if (typeof res.primary === "string" && res.primary.trim().length > 0) {
        return res.primary.trim();
      }
      if (res.isWritablePrimary === true || res.ismaster === true) {
        return host;
      }
    } catch {
      // Try next member if this node is unreachable.
    } finally {
      await probeClient.close().catch(() => undefined);
    }
  }
  return null;
}

export async function handleAuditRoute(
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  deps: {
    mongoClient: MongoClient;
    dbName: string;
    collectionName: string;
    unprocessedCol: Collection<UnprocessedMessageDoc>;
  },
): Promise<boolean> {
  if (!url.pathname.startsWith("/internal/audit/")) {
    return false;
  }

  const expectedToken = process.env.AUDIT_API_TOKEN;
  if (!verifyAuditBearerToken(req.headers.authorization, expectedToken)) {
    sendError(res, 401, "unauthorized", "token tidak valid");
    return true;
  }

  const db: Db = deps.mongoClient.db(deps.dbName);

  if (req.method === "GET" && url.pathname === "/internal/audit/events") {
    const source = url.searchParams.get("source");
    const sinceParam = url.searchParams.get("since");
    const limitParam = url.searchParams.get("limit");
    const cursorParam = url.searchParams.get("cursor");

    let limit = 100;
    if (limitParam !== null) {
      const parsed = Number(limitParam);
      if (!Number.isInteger(parsed) || parsed < 1 || parsed > 1000) {
        sendError(res, 400, "bad_request", "limit must be an integer between 1 and 1000");
        return true;
      }
      limit = parsed;
    }

    const filter: Record<string, unknown> = {};
    if (source) filter.source = source;
    if (sinceParam !== null) {
      const sinceDate = new Date(sinceParam);
      if (Number.isNaN(sinceDate.getTime())) {
        sendError(res, 400, "bad_request", "invalid since timestamp");
        return true;
      }
      filter.ingested_at = { $gte: sinceDate };
    }

    if (cursorParam) {
      const decoded = decodeCursor(cursorParam);
      if (!decoded) {
        sendError(res, 400, "bad_request", "invalid cursor");
        return true;
      }
      filter.$or = [
        { ingested_at: { $gt: decoded.t } },
        { ingested_at: decoded.t, hazard_id: { $gt: decoded.id } },
      ];
    }

    const result = await queryWithPartialFallback(
      deps.mongoClient,
      deps.dbName,
      deps.collectionName,
      (col, opts) =>
        col
        .find(filter, {
          allowPartialResults: opts.allowPartialResults,
          maxTimeMS: opts.maxTimeMS,
          comment: opts.comment,
        })
          .sort({ ingested_at: 1, hazard_id: 1 })
          .limit(limit + 1)
          .toArray(),
    );

    const hasMore = result.data.length > limit;
    const page = hasMore ? result.data.slice(0, limit) : result.data;
    const items = page.map((doc: any) => ({
      hazard_id: doc.hazard_id,
      source: doc.source,
      source_ref_id: doc.source_ref_id,
      hazard_type: doc.hazard_type,
      severity: doc.severity,
      area_name: doc.area_name,
      latitude: doc.latitude,
      longitude: doc.longitude,
      occurred_at: doc.occurred_at instanceof Date ? doc.occurred_at.toISOString() : doc.occurred_at,
      ingested_at: doc.ingested_at instanceof Date ? doc.ingested_at.toISOString() : doc.ingested_at,
      attributes: unwrapAttributes(doc.attributes),
    }));

    const last = page[page.length - 1];
    const next_cursor = hasMore && last ? encodeCursor(last.ingested_at, last.hazard_id) : null;

    sendJson(res, 200, {
      items,
      next_cursor,
      partial: result.partial,
      unavailable_units: result.unavailable_units,
    });
    return true;
  }

  if (req.method === "GET" && url.pathname === "/internal/audit/checksum") {
    const source = url.searchParams.get("source");
    const filter: Record<string, unknown> = {};
    if (source) filter.source = source;

    const result = await queryWithPartialFallback(
      deps.mongoClient,
      deps.dbName,
      deps.collectionName,
      (col, opts) =>
        col
          .find(filter, {
            projection: { _id: 0, source_ref_id: 1 },
            allowPartialResults: opts.allowPartialResults,
            maxTimeMS: opts.maxTimeMS,
          comment: opts.comment,
          })
          .toArray(),
      () => [],
    );

    const refIds = result.data.map((d: any) => String(d.source_ref_id ?? ""));
    const checksum = computeByteOrderChecksum(refIds);

    sendJson(res, 200, {
      source: source ?? "ALL",
      count: refIds.length,
      checksum,
      algorithm: "sha256",
      as_of: new Date().toISOString(),
      partial: result.partial,
      unavailable_units: result.unavailable_units,
    });
    return true;
  }

  if (req.method === "GET" && url.pathname === "/internal/audit/location") {
    const hazardId = url.searchParams.get("hazard_id")?.trim();
    if (!hazardId) {
      sendError(res, 400, "bad_request", "hazard_id is required");
      return true;
    }

    const col = db.collection(deps.collectionName);
    const doc = await col.findOne({ hazard_id: hazardId }, { maxTimeMS: 2500 });
    if (!doc) {
      sendError(res, 404, "not_found", "hazard_id not found");
      return true;
    }

    const configDb = deps.mongoClient.db("config");
    const namespace = `${deps.dbName}.${deps.collectionName}`;
    const collMeta = await configDb.collection("collections").findOne({ _id: namespace as any });
    let shardId = "unknown";
    if (collMeta?.uuid) {
      const chunk = await configDb.collection("chunks").findOne({
        uuid: collMeta.uuid,
        "min.partition_key": { $lte: doc.partition_key },
        "max.partition_key": { $gt: doc.partition_key },
      });
      if (chunk?.shard) shardId = String(chunk.shard);
    }

    const shardDoc = await configDb.collection("shards").findOne({ _id: shardId as any });
    const hostStr = String(shardDoc?.host ?? "");
    const hostsPart = hostStr.includes("/") ? hostStr.split("/")[1] : hostStr;
    const hosts = hostsPart ? hostsPart.split(",").map((h) => h.trim()).filter(Boolean) : [];
    const [podToNode, primaryHost] = await Promise.all([
      getPodToNodeMap(),
      hosts.length > 0 ? resolveShardPrimaryHost(deps.mongoClient, hostStr) : Promise.resolve(null),
    ]);

    const locations = buildShardLocations(shardId, hosts, primaryHost, podToNode);

    sendJson(res, 200, {
      hazard_id: hazardId,
      locations,
      partial: false,
    });
    return true;
  }

  if (req.method === "GET" && url.pathname === "/internal/audit/unprocessed") {
    const source = url.searchParams.get("source");
    const limitParam = url.searchParams.get("limit");
    const cursorParam = url.searchParams.get("cursor");

    let limit = 100;
    if (limitParam !== null) {
      const parsed = Number(limitParam);
      if (!Number.isInteger(parsed) || parsed < 1 || parsed > 1000) {
        sendError(res, 400, "bad_request", "limit must be an integer between 1 and 1000");
        return true;
      }
      limit = parsed;
    }

    const query: Record<string, unknown> = { status: { $in: ["retrying", "parked"] } };
    if (source) query.source = source;
    if (cursorParam) {
      const decoded = decodeCursor(cursorParam);
      if (!decoded) {
        sendError(res, 400, "bad_request", "invalid cursor");
        return true;
      }
      query.$or = [
        { received_at: { $gt: decoded.t } },
        { received_at: decoded.t, message_id: { $gt: decoded.id } },
      ];
    }

    const rows = await deps.unprocessedCol
      .find(query, {
        projection: {
          _id: 0,
          message_id: 1,
          source: 1,
          received_at: 1,
          reason: 1,
          retryable: 1,
          attempts: 1,
          status: 1,
        },
      })
      .sort({ received_at: 1, message_id: 1 })
      .limit(limit + 1)
      .toArray();

    const hasMore = rows.length > limit;
    const page = hasMore ? rows.slice(0, limit) : rows;
    const items = page.map((r) => ({
      ...r,
      received_at: r.received_at instanceof Date ? r.received_at.toISOString() : r.received_at,
    }));
    const last = page[page.length - 1];
    const next_cursor = hasMore && last ? encodeCursor(last.received_at, last.message_id) : null;

    sendJson(res, 200, { items, next_cursor });
    return true;
  }

  sendError(res, 404, "not_found", "audit endpoint not found");
  return true;
}
