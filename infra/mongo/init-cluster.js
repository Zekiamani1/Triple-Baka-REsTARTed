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
  const chunks = db.getSiblingDB("config").chunks;
  const boundaries = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"];
  for (const b of boundaries) {
    if (!chunks.findOne({ uuid: metadata.uuid, "min.partition_key": b })) {
      command(admin.runCommand({ split: namespace, middle: { partition_key: b, source: MinKey(), source_ref_id: MinKey() } }));
    }
  }
  const shards = ["shard1RS", "shard2RS"];
  const allChunks = chunks.find({ uuid: metadata.uuid }).sort({ min: 1 }).toArray();
  for (let i = 0; i < allChunks.length; i++) {
    const targetShard = shards[i < allChunks.length / 2 ? 0 : 1];
    if (allChunks[i].shard !== targetShard) {
      command(admin.runCommand({ moveChunk: namespace, find: allChunks[i].min, to: targetShard }));
    }
  }
  setup.updateOne({ _id: namespace }, { $set: { rangesReady: true } }, { upsert: true });
}
events.createIndex({ hazard_id: 1 });
events.createIndex({ ingested_at: 1, hazard_id: 1 });
events.createIndex({ hazard_type: 1, occurred_at: -1 });
events.createIndex({ severity: 1, occurred_at: -1 });
events.createIndex({ area_name: 1 });
events.createIndex({ ingested_at: -1, source: 1, source_ref_id: 1 });
print(`${namespace}: sharded with unique event identity; initial hash ranges distributed`);
