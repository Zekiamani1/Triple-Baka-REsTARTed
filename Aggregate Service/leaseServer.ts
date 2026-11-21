import type { IncomingMessage, ServerResponse } from "node:http";
import type { Db } from "mongodb";
import { logWarn } from "./telemetry";

export interface PollerLeaseDoc {
  _id: string;
  leader_id: string;
  fencing_token: number;
  lease_until: Date;
  cursor: Date | null;
  updated_at: Date;
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}

function sendError(res: ServerResponse, status: number, code: string, message: string): void {
  sendJson(res, status, { error: { code, message } });
}

async function readJsonBody(req: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  const text = Buffer.concat(chunks).toString("utf8").trim();
  return text ? (JSON.parse(text) as Record<string, unknown>) : {};
}

export async function validateFencingToken(
  db: Db,
  source: string,
  incomingTokenRaw: string | number | undefined,
): Promise<boolean> {
  if (source !== "BMKG" && source !== "PVMBG") return true;
  if (incomingTokenRaw === undefined || incomingTokenRaw === "") return true;
  const incomingToken = Number(incomingTokenRaw);
  if (!Number.isFinite(incomingToken)) return false;

  const lease = await db.collection<PollerLeaseDoc>("_poller_leases").findOne({ _id: source });
  if (!lease) return true;
  if (incomingToken < lease.fencing_token) {
    logWarn("stale_fencing_token rejected", {
      source,
      incoming_token: incomingToken,
      current_token: lease.fencing_token,
      current_leader: lease.leader_id,
    });
    return false;
  }
  return true;
}

export async function handleLeaseRoute(
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  db: Db,
): Promise<boolean> {
  if (!url.pathname.startsWith("/internal/leases/")) {
    return false;
  }

  const col = db.collection<PollerLeaseDoc>("_poller_leases");

  if (req.method === "POST" && url.pathname === "/internal/leases/acquire") {
    const body = await readJsonBody(req);
    const source = String(body.source ?? "").trim();
    const instanceId = String(body.instance_id ?? "").trim();
    const ttlSeconds = Math.max(Number(body.ttl_seconds ?? 15) || 15, 5);
    if (!source || !instanceId) {
      sendError(res, 400, "bad_request", "source and instance_id are required");
      return true;
    }

    const ttlMs = ttlSeconds * 1000;
    const updated = await col.findOneAndUpdate(
      { _id: source },
      [
        {
          $set: {
            _canTake: {
              $or: [
                { $eq: [{ $type: "$lease_until" }, "missing"] },
                { $lte: ["$lease_until", "$$NOW"] },
                { $eq: ["$leader_id", instanceId] },
              ],
            },
            _isRenewal: {
              $and: [
                { $eq: ["$leader_id", instanceId] },
                { $gt: ["$lease_until", "$$NOW"] },
              ],
            },
          },
        },
        {
          $set: {
            leader_id: { $cond: ["$_canTake", instanceId, "$leader_id"] },
            fencing_token: {
              $cond: [
                "$_canTake",
                {
                  $cond: [
                    "$_isRenewal",
                    { $ifNull: ["$fencing_token", 1] },
                    { $add: [{ $ifNull: ["$fencing_token", 0] }, 1] },
                  ],
                },
                "$fencing_token",
              ],
            },
            lease_until: {
              $cond: ["$_canTake", { $add: ["$$NOW", ttlMs] }, "$lease_until"],
            },
            cursor: { $ifNull: ["$cursor", null] },
            updated_at: "$$NOW",
          },
        },
        { $unset: ["_canTake", "_isRenewal"] },
      ],
      { upsert: true, returnDocument: "after" },
    );

    const doc = updated;
    const acquired = doc?.leader_id === instanceId;
    sendJson(res, 200, {
      acquired,
      leader_id: doc?.leader_id ?? instanceId,
      fencing_token: doc?.fencing_token ?? 1,
      cursor: doc?.cursor instanceof Date ? doc.cursor.toISOString() : doc?.cursor ?? null,
    });
    return true;
  }

  if (req.method === "POST" && url.pathname === "/internal/leases/cursor") {
    const body = await readJsonBody(req);
    const source = String(body.source ?? "").trim();
    const instanceId = String(body.instance_id ?? "").trim();
    const fencingToken = Number(body.fencing_token);
    const cursorRaw = String(body.cursor ?? "").trim();
    const cursorDate = new Date(cursorRaw);

    if (!source || !instanceId || !Number.isFinite(fencingToken) || Number.isNaN(cursorDate.getTime())) {
      sendError(res, 400, "bad_request", "source, instance_id, fencing_token, and valid cursor are required");
      return true;
    }

    const resUpdate = await col.updateOne(
      { _id: source, leader_id: instanceId, fencing_token: fencingToken },
      [{ $set: { cursor: cursorDate, updated_at: "$$NOW" } }],
    );

    if (resUpdate.matchedCount === 0) {
      sendError(res, 409, "stale_fencing_token", "lease not held by instance or fencing token is stale");
      return true;
    }

    sendJson(res, 200, { ok: true, cursor: cursorDate.toISOString() });
    return true;
  }

  sendError(res, 404, "not_found", "lease endpoint not found");
  return true;
}
