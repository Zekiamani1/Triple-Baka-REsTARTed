const command = require("/scripts/command.cjs");
const databaseName = process.env.MONGO_DB_NAME || "bnpb";
const collectionName = process.env.MONGO_COLLECTION || "hazard_events";
const namespace = `${databaseName}.${collectionName}`;
const admin = db.getSiblingDB("admin");
command(admin.runCommand({ setDefaultRWConcern: 1,
  defaultWriteConcern: { w: "majority", wtimeout: 10000 } }));
for (const [name, hosts] of [
  ["shard1RS", "mongo-shard-1:27018,mongo-shard-1-replica-2:27018,mongo-shard-1-replica-3:27018"],
  ["shard2RS", "mongo-shard-2:27018,mongo-shard-2-replica-2:27018,mongo-shard-2-replica-3:27018"],
]) {
  if (!db.getSiblingDB("config").shards.findOne({ _id: name }))
    command(admin.runCommand({ addShard: `${name}/${hosts}`, name }));
}
const events = db.getSiblingDB(databaseName).getCollection(collectionName);
const key = { partition_key: 1, source: 1, source_ref_id: 1 };
if (!db.getSiblingDB("config").collections.findOne({ _id: namespace, dropped: { $ne: true } })) {
  // Refuse to silently shard pre-existing documents lacking the application key.
  if (events.countDocuments({ partition_key: { $exists: false } }) !== 0)
    throw new Error("Collection contains documents without partition_key; migrate them before enabling sharding");
  events.createIndex(key, { unique: true });
  command(admin.runCommand({ shardCollection: namespace, key, unique: true }));
  // Persist progress separately: a rerun can finish after interruption.
}
const metadata = db.getSiblingDB("config").collections.findOne({ _id: namespace });
if (JSON.stringify(metadata.key) !== JSON.stringify(key))
  throw new Error("Unexpected existing shard key");
const setup = db.getSiblingDB(databaseName).getCollection("_cluster_setup");
if (!setup.findOne({ _id: namespace, rangesReady: true })) {
  const boundary = { partition_key: "8", source: MinKey(), source_ref_id: MinKey() };
  const chunks = db.getSiblingDB("config").chunks;
  if (!chunks.findOne({ uuid: metadata.uuid, "min.partition_key": "8" }))
    command(admin.runCommand({ split: namespace, middle: boundary }));
  const lower = chunks.find({ uuid: metadata.uuid }).sort({ min: 1 }).limit(1).next();
  if (lower.shard !== "shard1RS")
    command(admin.runCommand({ moveChunk: namespace,
      find: { partition_key: "0", source: "", source_ref_id: "" }, to: "shard1RS" }));
  const upper = chunks.findOne({ uuid: metadata.uuid, "min.partition_key": "8" });
  if (upper.shard !== "shard2RS")
    command(admin.runCommand({ moveChunk: namespace,
      find: { partition_key: "9", source: "", source_ref_id: "" }, to: "shard2RS" }));
  setup.updateOne({ _id: namespace }, { $set: { rangesReady: true } }, { upsert: true });
}
events.createIndex({ hazard_id: 1 });
events.createIndex({ ingested_at: -1, source: 1, source_ref_id: 1 });
print(`${namespace}: sharded with unique event identity; initial hash ranges distributed`);
