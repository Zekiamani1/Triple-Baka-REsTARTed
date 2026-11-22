#!/usr/bin/env bash
set -euo pipefail
NS="${K8S_NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
echo "=== Kubernetes MongoDB Pods ==="
kubectl get pods -n "$NS" -l app.kubernetes.io/part-of=percona-server-mongodb -o wide
echo ""
echo "=== Shard Distribution ==="
kubectl exec -n "$NS" deploy/aggregate -- node -e '
const { MongoClient } = require("mongodb");
(async () => {
  const c = new MongoClient(process.env.MONGO_URI);
  await c.connect();
  const cfg = c.db("config");
  const shards = await cfg.collection("shards").find().toArray();
  const chunks = await cfg.collection("chunks").aggregate([{ $group: { _id: "$shard", chunks: { $sum: 1 } } }]).toArray();
  console.log("Shards:", shards.map(s => s._id));
  console.log("Chunks per shard:", chunks);
  await c.close();
})();
'
