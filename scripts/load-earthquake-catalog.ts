import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { Kafka } from "kafkajs";

const filePath = process.argv[2];
if (!filePath) {
  console.error("Usage: tsx scripts/load-earthquake-catalog.ts <catalog.json|catalog.csv>");
  process.exit(1);
}

const raw = readFileSync(filePath, "utf8").trim();
let rows: Array<Record<string, any>> = [];
if (raw.startsWith("[")) {
  rows = JSON.parse(raw);
} else {
  const lines = raw.split(/\r?\n/);
  const headers = lines[0].split(",").map((h) => h.trim());
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = lines[i].split(",");
    const obj: Record<string, string> = {};
    headers.forEach((h, idx) => { obj[h] = (cols[idx] ?? "").trim(); });
    rows.push(obj);
  }
}

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
const topic = process.env.KAFKA_TOPIC_HAZARD_INGEST ?? "hazard-ingest";
const kafka = new Kafka({ clientId: "catalog-loader", brokers });
const producer = kafka.producer();

async function main() {
  await producer.connect();
  const start = performance.now();
  const batchSize = 500;
  for (let i = 0; i < rows.length; i += batchSize) {
    const slice = rows.slice(i, i + batchSize);
    const messages = slice.map((r, idx) => {
      const refId = String(r.source_ref_id ?? r.event_id ?? r.id ?? `eq-${i + idx}`);
      const mag = Number(r.magnitude ?? r.mag ?? 4.0);
      const severity = mag >= 6.5 ? "AWAS" : mag >= 5.5 ? "SIAGA" : mag >= 4.5 ? "WASPADA" : "NORMAL";
      const ev = {
        hazard_id: randomUUID(),
        source: "BMKG",
        source_ref_id: refId,
        hazard_type: "SEISMIC",
        severity,
        area_name: String(r.area_name ?? r.region ?? r.place ?? "Indonesia"),
        latitude: Number(r.latitude ?? r.lat ?? 0),
        longitude: Number(r.longitude ?? r.lon ?? r.lng ?? 0),
        occurred_at: new Date(r.occurred_at ?? r.time ?? Date.now()).toISOString(),
        ingested_at: new Date().toISOString(),
        attributes: { magnitude: mag, depth_km: Number(r.depth_km ?? r.depth ?? 10) },
      };
      const key = createHash("sha256").update(JSON.stringify(["BMKG", refId])).digest("hex");
      return { key, value: JSON.stringify(ev) };
    });
    await producer.send({ topic, messages });
  }
  const sec = (performance.now() - start) / 1000;
  console.log(`Loaded ${rows.length} events in ${sec.toFixed(2)}s (${(rows.length / Math.max(sec, 0.001)).toFixed(1)} events/s)`);
  await producer.disconnect();
}

main().catch((err) => { console.error(err); process.exit(1); });
