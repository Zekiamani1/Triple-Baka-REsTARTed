#!/usr/bin/env bash
set -euo pipefail
NS="${K8S_NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
BASE_URLS="${BASE_URLS:-http://localhost:30080}"

echo "=== P2 Scenario 1: Sustained 10,000 VUs for 120s with live scaling 3 -> 5 -> 2 ==="
BASE_URLS="$BASE_URLS" TARGET_VUS=10000 DURATION=120s k6 run "Load testing/loadtest.js" &
K6_PID=$!
sleep 30
kubectl scale deploy/aggregate -n "$NS" --replicas=5 || true
sleep 30
kubectl scale deploy/aggregate -n "$NS" --replicas=2 || true
wait "$K6_PID"

echo "=== P2 Scenario 2: 1 instance vs 3 instances throughput comparison ==="
kubectl scale deploy/aggregate -n "$NS" --replicas=1 && kubectl rollout status deploy/aggregate -n "$NS"
BASE_URLS="$BASE_URLS" TARGET_VUS=2000 DURATION=30s k6 run "Load testing/loadtest.js"
kubectl scale deploy/aggregate -n "$NS" --replicas=3 && kubectl rollout status deploy/aggregate -n "$NS"
BASE_URLS="$BASE_URLS" TARGET_VUS=2000 DURATION=30s k6 run "Load testing/loadtest.js"

echo "=== P2 Scenario 3: Zero-downtime rolling upgrade during load ==="
BASE_URLS="$BASE_URLS" TARGET_VUS=1000 DURATION=45s k6 run "Load testing/loadtest.js" &
K6_ROLL_PID=$!
sleep 10
kubectl rollout restart deploy/aggregate -n "$NS"
kubectl rollout status deploy/aggregate -n "$NS"
wait "$K6_ROLL_PID"
