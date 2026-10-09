# System Triple Baka
![alt text](triplebaka.gif)

The Triple Baka System is a distributed disaster coordination platform that collects and processes disaster information from external systems. BMKG and PVMBG act as external data sources that provide disaster information, while BNPB (internal system) serves as the central coordination hub that retrieves data from both institutions, processes it, and unifies it into a canonical format.

BNPB periodically polls both sources and maps the retrieved data into a standardized data format. The mapped data is stored in a Canonical Store, from which it can be served to downstream clients or distributed through a message broker to various consumers. Each service has separate storage and access, and is packaged as a container that can be run and tested independently.


## Requirements

- Docker Engine
- Docker Compose v2
- Optional: `curl` and `jq` for trying the API

## Run locally

Create local environment files from the provided examples:

```bash
cp 'Auth Service/.env.example' 'Auth Service/.env'
cp 'External System/BMKG/.env.example' 'External System/BMKG/.env'
cp 'External System/PVMBG/.env.example' 'External System/PVMBG/.env'
cp 'Poller Service/BMKG Poller Service/.env.example' 'Poller Service/BMKG Poller Service/.env'
cp 'Poller Service/PVMBG Poller Service/.env.example' 'Poller Service/PVMBG Poller Service/.env'
```

Set the client secrets and source API keys in those `.env` files. The BMKG API
key must match the key configured in BMKG Poller; the PVMBG API key must match
the key configured in PVMBG Poller.

Build and run:

```bash
docker compose up --build -d
```

Stop program:

```bash
docker compose down
```

## Client API

The public API is exposed by the Gateway at `http://localhost:8080`.

| Client | Client ID | Scope | Access |
| --- | --- | --- | --- |
| Media/Pers | `media` | `hazard:read:summary` | Summary fields only |
| Field team | `lapangan` | `hazard:read:full` | Full HazardEvent |
| BNPB internal | `internal` | `hazard:read:full` | Full HazardEvent |

### Get an access token

Replace `<client-id>` with `media`, `lapangan`, or `internal`, and replace
`<client-secret>` with the corresponding secret (on .env).

```bash
curl -X POST http://localhost:8080/auth/token \
  -H 'Content-Type: application/json' \
  --data '{
    "clientId": "<client-id>",
    "clientSecret": "<client-secret>"
  }'
```

The response contains `accessToken`, `refreshToken`, and `expiresIn`.

### List HazardEvents

```bash
curl -H 'Authorization: Bearer <access-token>' \
  'http://localhost:8080/aggregate/hazards?limit=20'
```

Optional filters: `source`, `hazard_type`, `severity`, `limit`, and `offset`.

```bash
# PVMBG volcanic events
curl -H 'Authorization: Bearer <access-token>' \
  'http://localhost:8080/aggregate/hazards?source=PVMBG&hazard_type=VOLCANIC&limit=20'

# One event
curl -H 'Authorization: Bearer <access-token>' \
  'http://localhost:8080/aggregate/hazards/<hazard-id>'
```

### Refresh an access token

```bash
curl -X POST http://localhost:8080/auth/refresh \
  -H 'Content-Type: application/json' \
  --data '{"refreshToken":"<refresh-token>"}'
```

## Source API and demo controls

These endpoints are intended for local development and demonstrations. Source
credentials are different from client credentials.

### BMKG

```bash
curl -H 'X-BMKG-Key: <bmkg-api-key>' \
  'http://localhost:8081/seismic-events?since=2026-01-01T00:00:00Z'

curl -H 'X-BMKG-Key: <bmkg-api-key>' \
  'http://localhost:8081/tsunami-warnings?since=2026-01-01T00:00:00Z'
```

### PVMBG

```bash
curl -H 'Authorization: Bearer <pvmbg-api-key>' \
  'http://localhost:8082/volcanic-reports?since=2026-01-01T00:00:00Z'
```

Enable or disable an outage:

```bash
curl -X POST http://localhost:8082/admin/outage \
  -H 'Authorization: Bearer <pvmbg-api-key>' \
  -H 'Content-Type: application/json' \
  --data '{"enabled":true}'

curl -X POST http://localhost:8082/admin/outage \
  -H 'Authorization: Bearer <pvmbg-api-key>' \
  -H 'Content-Type: application/json' \
  --data '{"enabled":false}'
```

Enable the additive `confidence_level` field in PVMBG responses while the
system is running:

```bash
curl -X POST http://localhost:8082/admin/schema-version \
  -H 'Authorization: Bearer <pvmbg-api-key>'
```

After one polling interval, query `/aggregate/hazards` through the Client API
to see the resulting canonical event.

## Ports

| Service | Port |
| --- | --- |
| API Gateway | `8080` |
| BMKG | `8081` |
| PVMBG | `8082` |
| Auth Service (gRPC) | `8083` |
| Aggregator (gRPC) | `50051` |
| Kafka | `9092` |
| MongoDB | `27017` |
| Envoy admin | `9901` |