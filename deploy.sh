#!/usr/bin/env bash
set -euo pipefail

NS="sistem-informasi-kebencanaan-bnpb"
WITH_TESTER_AGENT=false

for arg in "$@"; do
  case "$arg" in
    --with-tester-agent)
      WITH_TESTER_AGENT=true
      ;;
  esac
done

command -v kubectl >/dev/null 2>&1 || { echo "kubectl is required" >&2; exit 1; }
command -v helm >/dev/null 2>&1 || { echo "helm is required" >&2; exit 1; }

for envfile in \
  "Auth Service/.env" \
  "External System/BMKG/.env" \
  "External System/PVMBG/.env" \
  "Poller Service/BMKG Poller Service/.env" \
  "Poller Service/PVMBG Poller Service/.env" \
  "Aggregate Service/.env" \
  "Event Consumer/.env" \
  "Alert Consumer/.env"; do
  if [[ ! -f "$envfile" ]]; then
    echo "Missing required env file: $envfile" >&2
    exit 1
  fi
done

if ! helm status psmdb-operator -n psmdb-operator >/dev/null 2>&1; then
  echo "Installing Percona Server for MongoDB Operator v1.19.0..."
  helm repo add percona https://percona.github.io/percona-helm-charts/ >/dev/null
  helm repo update >/dev/null
  helm upgrade --install psmdb-operator percona/psmdb-operator \
    --version 1.19.0 \
    --namespace psmdb-operator \
    --create-namespace \
    --wait
fi

echo "Applying Kustomize manifests to namespace $NS..."
kubectl apply -k .

echo "Waiting for deployments and statefulsets..."
kubectl rollout status statefulset/auth-db -n "$NS" --timeout=300s
kubectl rollout status deploy/kafka-aggregate -n "$NS" --timeout=300s
kubectl rollout status deploy/auth-service -n "$NS" --timeout=300s
kubectl rollout status deploy/bmkg -n "$NS" --timeout=300s
kubectl rollout status deploy/pvmbg -n "$NS" --timeout=300s
kubectl rollout status deploy/aggregate -n "$NS" --timeout=300s
kubectl rollout status deploy/bmkg-poller -n "$NS" --timeout=300s
kubectl rollout status deploy/pvmbg-poller -n "$NS" --timeout=300s
kubectl rollout status deploy/event-consumer -n "$NS" --timeout=300s
kubectl rollout status deploy/alert-consumer -n "$NS" --timeout=300s
kubectl rollout status deploy/gateway -n "$NS" --timeout=300s
kubectl rollout status deploy/analytics-dashboard -n "$NS" --timeout=300s
kubectl rollout status deploy/otel-lgtm -n "$NS" --timeout=300s || true

if [[ "$WITH_TESTER_AGENT" == "true" ]]; then
  echo "Applying official Tester Agent manifest..."
  kubectl apply -f "https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml"
fi

echo "Deployment complete!"
