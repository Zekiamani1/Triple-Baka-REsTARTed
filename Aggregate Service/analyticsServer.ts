import type { IncomingMessage, ServerResponse } from "node:http";
import type { MongoClient } from "mongodb";
import { unwrapAttributes } from "./auditServer";
import { queryWithPartialFallback } from "./partialQuery";

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}

function sendError(res: ServerResponse, status: number, code: string, message: string): void {
  sendJson(res, status, { error: { code, message } });
}

function pickInterval(fromDate?: Date, toDate?: Date, requested?: string | null): "minute" | "hour" | "day" {
  if (requested === "minute" || requested === "hour" || requested === "day") return requested;
  if (!fromDate || !toDate) return "hour";
  const spanMs = toDate.getTime() - fromDate.getTime();
  if (spanMs <= 3 * 3600 * 1000) return "minute";
  if (spanMs <= 48 * 3600 * 1000) return "hour";
  return "day";
}

function bucketKey(d: Date, interval: "minute" | "hour" | "day"): string {
  const iso = d.toISOString();
  if (interval === "minute") return iso.slice(0, 16) + ":00.000Z";
  if (interval === "hour") return iso.slice(0, 13) + ":00:00.000Z";
  return iso.slice(0, 10) + "T00:00:00.000Z";
}

export async function handleAnalyticsRoute(
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  deps: { mongoClient: MongoClient; dbName: string; collectionName: string },
): Promise<boolean> {
  if (req.method !== "GET" || url.pathname !== "/analytics/summary") {
    return false;
  }

  const fromParam = url.searchParams.get("from");
  const toParam = url.searchParams.get("to");
  const intervalParam = url.searchParams.get("interval");

  let fromDate: Date | undefined;
  let toDate: Date | undefined;
  if (fromParam) {
    fromDate = new Date(fromParam);
    if (Number.isNaN(fromDate.getTime())) {
      sendError(res, 400, "bad_request", "invalid from timestamp");
      return true;
    }
  }
  if (toParam) {
    toDate = new Date(toParam);
    if (Number.isNaN(toDate.getTime())) {
      sendError(res, 400, "bad_request", "invalid to timestamp");
      return true;
    }
  }

  const filter: Record<string, unknown> = {};
  if (fromDate || toDate) {
    const range: Record<string, Date> = {};
    if (fromDate) range.$gte = fromDate;
    if (toDate) range.$lte = toDate;
    filter.occurred_at = range;
  }

  const result = await queryWithPartialFallback(
    deps.mongoClient,
    deps.dbName,
    deps.collectionName,
    (col, opts) =>
      col
        .find(filter, {
          projection: {
            _id: 0,
            hazard_type: 1,
            severity: 1,
            area_name: 1,
            occurred_at: 1,
            attributes: 1,
          },
          allowPartialResults: opts.allowPartialResults,
          maxTimeMS: opts.maxTimeMS,
        })
        .toArray(),
  );

  const interval = pickInterval(fromDate, toDate, intervalParam);
  const byHazardType: Record<string, number> = {
    SEISMIC: 0,
    TSUNAMI: 0,
    VOLCANIC: 0,
    WILDFIRE: 0,
    COASTAL: 0,
  };
  const byCanonicalHazardType: Record<string, number> = {
    SEISMIC: 0,
    VOLCANIC: 0,
    WILDFIRE: 0,
    COASTAL: 0,
  };
  const bySeverity: Record<string, number> = {
    NORMAL: 0,
    WASPADA: 0,
    SIAGA: 0,
    AWAS: 0,
  };
  const areaCounts = new Map<string, number>();
  const seriesMap = new Map<string, Record<string, number>>();

  for (const doc of result.data as any[]) {
    const canonicalType = String(doc.hazard_type ?? "SEISMIC");
    if (canonicalType in byCanonicalHazardType) {
      byCanonicalHazardType[canonicalType]++;
    } else {
      byCanonicalHazardType[canonicalType] = 1;
    }

    const attrs = unwrapAttributes(doc.attributes);
    const isTsunami =
      canonicalType === "SEISMIC" &&
      (attrs.potential_tsunami === true || Boolean(attrs.warning_id));
    const displayType = isTsunami ? "TSUNAMI" : canonicalType;
    byHazardType[displayType] = (byHazardType[displayType] ?? 0) + 1;

    const sev = String(doc.severity ?? "NORMAL");
    bySeverity[sev] = (bySeverity[sev] ?? 0) + 1;

    const area = String(doc.area_name ?? "Unknown");
    areaCounts.set(area, (areaCounts.get(area) ?? 0) + 1);

    const occ = doc.occurred_at instanceof Date ? doc.occurred_at : new Date(doc.occurred_at);
    if (!Number.isNaN(occ.getTime())) {
      const b = bucketKey(occ, interval);
      let row = seriesMap.get(b);
      if (!row) {
        row = { SEISMIC: 0, TSUNAMI: 0, VOLCANIC: 0, WILDFIRE: 0, COASTAL: 0 };
        seriesMap.set(b, row);
      }
      row[displayType] = (row[displayType] ?? 0) + 1;
    }
  }

  const time_series = Array.from(seriesMap.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([bucket, counts]) => ({ bucket, ...counts }));

  const by_area = Array.from(areaCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([area_name, count]) => ({ area_name, count }));

  sendJson(res, 200, {
    total_events: result.data.length,
    by_hazard_type: byHazardType,
    by_canonical_hazard_type: byCanonicalHazardType,
    time_series,
    by_severity: bySeverity,
    by_area,
    partial: result.partial,
    unavailable_units: result.unavailable_units,
    as_of: new Date().toISOString(),
  });
  return true;
}
