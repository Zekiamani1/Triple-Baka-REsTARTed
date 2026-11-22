#!/usr/bin/env bash
set -euo pipefail
NS="${K8S_NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
echo "Checking shard1 and shard2 primary node placement..."
kubectl get pods -n "$NS" -l app.kubernetes.io/part-of=percona-server-mongodb -o custom-columns=NAME:.metadata.name,NODE:.spec.nodeName,REPLSET:.metadata.labels."app\.kubernetes\.io/replset"
