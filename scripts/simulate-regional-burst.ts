import { createHash, randomUUID } from "node:crypto";
import { Kafka } from "kafkajs";

const count = Number(process.env.BURST_COUNT ?? "2000");
const area = process.env.BURST_AREA ?? "Aceh";
const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
const topic = process.env.KAFKA_TOPIC_HAZARD_INGEST ?? "hazard-ingest";

const kafka = new Kafka({ clientId: "regional-burst-sim", brokers });
const producer = kafka.producer();

async function main() {
  await producer.connect();
  const shards = { shard1_half: 0, shard2_half: 0 };
  const batchSize = 500;
  for (let i = 0; i < count; i += batchSize) {
    const messages = [];
    for (let j = i; j < Math.min(i + batchSize, count); j++) {
      const refId = `burst-${area}-${Date.now()}-${j}`;
      const pkey = createHash("sha256").update(JSON.stringify(["BMKG", refId])).digest("hex");
      if (pkey < "8") shards.shard1_half++;
      else shards.shard2_half++;
      messages.push({
        key: pkey,
        value: JSON.stringify({
          hazard_id: randomUUID(),
          source: "BMKG",
          source_ref_id: refId,
          hazard_type: "SEISMIC",
          severity: "SIAGA",
          area_name: area,
          latitude: 5.55,
          longitude: 95.32,
          occurred_at: new Date().toISOString(),
          ingested_at: new Date().toISOString(),
          attributes: { burst: true, seq: j },
        }),
      });
    }
    await producer.send({ topic, messages });
  }
  console.log(`Simulated ${count} events in "${area}". Hash distribution -> lower (<8): ${shards.shard1_half}, upper (>=8): ${shards.shard2_half}`);
  await producer.disconnect();
}

main().catch((err) => { console.error(err); process.exit(1); });
