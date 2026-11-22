# System Triple Baka
![alt text](triplebaka.gif)

The Triple Baka System is a distributed disaster coordination platform that collects, validates, deduplicates, and serves canonical disaster events (`HazardEvent`) for BNPB from multiple internal and external sources:

- **BMKG** (seismic & tsunami polling via `bmkg-poller`)
- **PVMBG** (volcanic activity & additive schema evolution via `pvmbg-poller`)
- **Satellite Wildfire Feed (Problem 4)** (`satellite-poller`: durable SQLite inbox/outbox, hotspot spatiotemporal clustering, `upsert`/`retract` tombstones, and Indonesian province mapping)
- **Sensor Pantai & Field Receiver (Problem 5)** (`coastal-adapter` + `field-receiver`: cursor checkpointing, circuit breaker, quarantine topic `pantai-quarantine`, `data_status` freshness reporting, and critical alert delivery)
- **Aggregate Service, Consumers, Gateway & Observability**: Envoy API Gateway (JWT + Lua field filtering + per-client rate limiting), split `aggregate` (API) & `aggregate-ingester` (Kafka consumer backed by Percona Server for MongoDB sharded cluster), `event-consumer`, `alert-consumer`, Analytics Dashboard, Internal Audit API (`/internal/audit/*`), and OpenTelemetry LGTM stack (`otel-lgtm`).

## Requirements

- **Docker Compose:** Docker Engine and Docker Compose v2
- **Kubernetes:** A Kubernetes distribution, `kubectl`, and `helm` (Percona MongoDB Operator v1.19.0)
- **Optional:** `curl` and `jq` for API testing

## Run locally

Create local `.env` files from the provided examples before starting Docker Compose or Kubernetes:

```bash
cp .env.example .env
cp 'Aggregate Service/.env.example' 'Aggregate Service/.env'
cp 'Event Consumer/.env.example' 'Event Consumer/.env'
cp 'Alert Consumer/.env.example' 'Alert Consumer/.env'
cp 'Auth Service/.env.example' 'Auth Service/.env'
cp 'External System/BMKG/.env.example' 'External System/BMKG/.env'
cp 'External System/PVMBG/.env.example' 'External System/PVMBG/.env'
cp 'Poller Service/BMKG Poller Service/.env.example' 'Poller Service/BMKG Poller Service/.env'
cp 'Poller Service/PVMBG Poller Service/.env.example' 'Poller Service/PVMBG Poller Service/.env'
cp 'Poller Service/Satellite Poller Service/.env.example' 'Poller Service/Satellite Poller Service/.env'
cp 'Poller Service/Coastal Poller Service/.env.example' 'Poller Service/Coastal Poller Service/.env'
cp 'Field Receiver/.env.example' 'Field Receiver/.env'
```

Update secrets cleanly across matching pairs:
- `BMKG` API key matches `BMKG Poller Service`.
- `PVMBG` API key matches `PVMBG Poller Service`.
- `SATELLITE_GROUP_TOKEN` matches the Satellite Feed session token (`Poller Service/Satellite Poller Service/.env`).
- `PANTAI_AUTH_TOKEN` matches between `sensor-pantai`, `Poller Service/Coastal Poller Service/.env`, and K8s `Secret/pantai-token`.
- `FIELD_RECEIVER_TOKEN` matches between `Alert Consumer/.env` and `Field Receiver/.env`.
- `AUDIT_API_TOKEN` in `.env.example` / `Secret/audit-secret` protects `/internal/audit/*`.

### Option 1: Run with Docker Compose

Build and run all services:

```bash
docker compose up --build -d
```

Stop the stack:

```bash
docker compose down
```

### Option 2: Run with Kubernetes

1. **Build the local service images** (into your cluster runtime or Docker daemon):

```bash
docker compose build
```

2. **Deploy using `./deploy.sh`** (installs Percona Operator v1.19.0 if missing, provisions `pantai-token`, generates adapter/receiver DB secrets, applies Kustomize, and waits for all rollouts):

```bash
export PANTAI_AUTH_TOKEN="<sensor-pantai-token>"
export AUDIT_API_TOKEN="<audit-bearer-token>" # optional; defaults to .env.example
./deploy.sh
# Or include the official tester agent:
./deploy.sh --with-tester-agent
```

Or apply Kustomize directly (after creating `pantai-token`, `pantai-adapter-db`, `field-receiver-db`, `field-receiver-token`):

```bash
kubectl apply -k .
```

3. **Access services via NodePort or port-forward:**

- **API Gateway:** NodePort `30080` (`kubectl port-forward -n sistem-informasi-kebencanaan-bnpb svc/gateway 8080:8080`)
- **Analytics Dashboard:** NodePort `30081` (`kubectl port-forward -n sistem-informasi-kebencanaan-bnpb svc/analytics-dashboard 30081:80`)
- **Grafana (`otel-lgtm`):** NodePort `30300` (`kubectl port-forward -n sistem-informasi-kebencanaan-bnpb svc/otel-lgtm 3000:3000`)

4. **Remove deployment:**

```bash
kubectl delete -k .
```

## Client API

The public API is exposed by Envoy Gateway at `http://localhost:8080`.

| Client | Client ID | Scope | Access |
| --- | --- | --- | --- |
| Media/Pers | `media` | `hazard:read:summary` | Summary fields only (50 req/s bucket) |
| Field team | `lapangan` | `hazard:read:full` | Full `HazardEvent` (2000 req/s bucket) |
| BNPB internal | `internal` | `hazard:read:full` | Full `HazardEvent` (1000 req/s bucket) |

### Get an access token

```bash
curl -X POST http://localhost:8080/auth/token \
  -H 'Content-Type: application/json' \
  --data '{
    "clientId": "<client-id>",
    "clientSecret": "<client-secret>"
  }'
```

### List HazardEvents

```bash
curl -H 'Authorization: Bearer <access-token>' \
  'http://localhost:8080/aggregate/hazards?limit=20'
```

Supported query parameters include `source` (`BMKG`, `PVMBG`, `SATELIT`, `SENSOR_PANTAI`), `hazard_type`, `severity`, `limit`, and `offset`. Note that `GET /aggregate/hazards` exposes HTTP header `X-Data-Status` (`fresh`, `stale`, `unavailable`) reflecting Coastal source freshness.

### Analytics & Audit Endpoints

- **Analytics Summary** (`http://localhost:8080/analytics/summary`, requires JWT): aggregated counts by source, severity, hazard type, and area (`Analytics Dashboard` runs on `:30081`).
- **Internal Audit API** (`svc/aggregate:50052`, `Authorization: Bearer <AUDIT_API_TOKEN>`):
  - `GET /internal/audit/hazards?source=&since=&until=&limit=&cursor=`
  - `GET /internal/audit/checksum?source=&since=&until=`
  - `GET /internal/audit/location?hazard_id=<hazard-id>`
  - `GET /internal/audit/unprocessed?source=&limit=&cursor=`
- **Satellite Poller Stats & Health** (`svc/satellite-poller:8087`): `/stats`, `/health/live`, `/health/ready`
- **Coastal Adapter Status & Health** (`svc/coastal-adapter:8089`): `/internal/source-status`, `/healthz`, `/readyz`

### Refresh an access token

```bash
curl -X POST http://localhost:8080/auth/refresh \
  -H 'Content-Type: application/json' \
  --data '{"refreshToken":"<refresh-token>"}'
```

## Ports

| Service | Port | Notes |
| --- | --- | --- |
| API Gateway | `8080` | Public REST + `/analytics/summary` |
| Envoy Admin | `9901` | Local debug/stats |
| Analytics Dashboard | `30081` (K8s/Compose) | Frontend UI |
| Auth Service (gRPC) | `8083` | Token issue / refresh |
| Aggregate Service (gRPC) | `50051` | Internal/Gateway gRPC API |
| Aggregate Audit/Analytics HTTP | `50052` | `/internal/audit/*`, `/analytics/summary` |
| BMKG Mock | `8081` | Simulated seismic/tsunami API |
| PVMBG Mock | `8082` | Simulated volcanic API |
| BMKG Poller Health | `8085` | `/health` |
| PVMBG Poller Health | `8086` | `/health` |
| Satellite Poller Health/Stats | `8087` | `/health/live`, `/health/ready`, `/stats` |
| Event Consumer Health | `8087` | `/health` |
| Alert Consumer Health | `8088` | `/health` |
| Coastal Adapter Health/Status | `8089` | `/healthz`, `/readyz`, `/internal/source-status` |
| Field Receiver API | `8090` | `/notifications`, `/receipts`, `/readyz` |
| Sensor Pantai Mock | `8080` (internal) | Upstream coastal telemetry feed |
| Kafka | `9092` / `29092` | External / internal broker listeners |
| MongoDB Router (`mongos`) | `27020` -> `27017` | Sharded cluster router |
| Grafana / OTLP (`otel-lgtm`) | `3000` / `4317` | Observability UI (`30300` NodePort) & OTLP gRPC |
