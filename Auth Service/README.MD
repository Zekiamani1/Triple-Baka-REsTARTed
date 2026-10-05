# Auth Service

![true...](https://media1.tenor.com/m/32U4Jym7dHgAAAAC/osaka-azumanga-daioh.gif)

Issues and verifies access tokens for the BNPB disaster-coordination platform (IF4031 Milestone 1, Problem 3). This service owns one trust domain: downstream clients (Media/Pers, Tim Lapangan, BNPB Internal) authenticating to get a scoped access token. It does not talk to BMKG or PVMBG, and it is not the service that enforces field-level (Ringkasan/Mentah) authorization — that lives in the Client-Facing API, which verifies tokens issued here.

API: gRPC only. No REST endpoints except `/health`.

## How it works, in short

1. A client (`media`, `lapangan`, or `internal`) calls `IssueToken` with its `client_id`/`client_secret`.
2. On success it gets back a short-lived **access token** (a signed JWT, default 60s) and a longer-lived **refresh token** (a random opaque string, default 24h).
3. The access token's `scope` claim is fixed by which client authenticated — never requestable. `media` always gets `hazard:read:summary`; `lapangan` and `internal` get `hazard:read:full`.
4. When the access token expires, the client calls `RefreshToken` with its refresh token to get a new pair. The old refresh token is invalidated the moment it's used (rotation) — reusing it returns `Unauthenticated`.
5. Any other service that needs to verify a token (the Client-Facing API) calls `GetJWKS` once at startup, caches the public key, and verifies tokens locally — it never calls this service per request.

## Prerequisites

- Docker and Docker Compose
- [`grpcurl`](https://github.com/fullstorydev/grpcurl) and `jq`, for calling the API and running the smoke test
- Go 1.22+, only if you want to run the service outside Docker

## Setup

```bash
cp .env.example .env
```

Fill in `.env` with real values — never commit this file:

| Variable | What it is |
|---|---|
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | Credentials for the `auth-db` container |
| `AUTH_DB_DSN` | Must point at the same user/password/db as above, e.g. `postgres://auth_user:<password>@auth-db:5432/auth?sslmode=disable` |
| `CLIENT_MEDIA_SECRET`, `CLIENT_LAPANGAN_SECRET`, `CLIENT_INTERNAL_SECRET` | One secret per client identity |
| `AUTH_JWT_PRIVATE_KEY_PATH` | Where the signing key lives inside the container; leave as `./keys/private.pem` |
| `AUTH_PORT` / `AUTH_HEALTH_PORT` | Default `8083` / `8084` |
| `AUTH_ACCESS_TOKEN_TTL_SECONDS` / `AUTH_REFRESH_TOKEN_TTL_SECONDS` | Default `60` / `86400` |
| `AUTH_ISSUER` | The `iss` claim embedded in every token; default `bnpb-auth` |
| `AUTH_ENABLE_REFLECTION` | Leave `false` except for local debugging — see Security notes |

Generate the three client secrets (and the Postgres password, if you haven't already):

```bash
openssl rand -base64 32   # run once per secret
```

Do **not** hand-write a signing key — it's generated automatically on first start (see Running, below).

## Running

```bash
docker compose up --build -d
docker compose ps
```

You should see four services settle: `auth-db` (healthy), `auth-seed` and `auth-keygen` (exited 0 — these are one-shot setup steps, not long-running), and `auth-service` (healthy). If `auth-service` doesn't become healthy, check logs in this order, since each one depends on the last:

```bash
docker compose logs auth-db --tail=20
docker compose logs auth-keygen --tail=20
docker compose logs auth-seed --tail=20
docker compose logs auth-service --tail=20
```

What each service does:
- **`auth-db`** — Postgres; schema is applied automatically from `migrations/` on first start only (an existing data volume is not re-migrated).
- **`auth-keygen`** — runs `./auth-service keygen` once. Creates `keys/private.pem` in a shared volume if it doesn't already exist; never overwrites one that does. Safe to run on every `up`.
- **`auth-seed`** — inserts/updates the three client rows (`media`, `lapangan`, `internal`) with bcrypt-hashed secrets from `.env`.
- **`auth-service`** — the actual gRPC server, on port `8083`, plus an HTTP `/health` check on `8084` (not published to the host — only `8083` is).

To wipe everything and start clean (new DB, new signing key):

```bash
docker compose down -v
```

## API reference

Proto: `proto/auth/v1/auth.proto`, service `auth.v1.AuthService`.

### `IssueToken`

```bash
set -a; source .env; set +a

grpcurl -plaintext -import-path . -proto proto/auth/v1/auth.proto \
  -d "{\"client_id\":\"media\",\"client_secret\":\"$CLIENT_MEDIA_SECRET\"}" \
  localhost:8083 auth.v1.AuthService/IssueToken
```

```json
{
  "accessToken": "eyJhbGciOiJSUzI1NiIs...",
  "refreshToken": "qV0b1tiqFvz9r1MiB6dKW...",
  "expiresIn": "60"
}
```

Client IDs and what they get:

| `client_id` | Scope | Represents |
|---|---|---|
| `media` | `hazard:read:summary` | Media / Pers |
| `lapangan` | `hazard:read:full` | Tim Lapangan |
| `internal` | `hazard:read:full` | BNPB Pusat / Internal Ops |

A wrong secret and an unknown `client_id` both return the identical `Unauthenticated` error — this is deliberate, not a bug, so don't rely on the error message to tell them apart.

### `RefreshToken`

```bash
grpcurl -plaintext -import-path . -proto proto/auth/v1/auth.proto \
  -d "{\"refresh_token\":\"<token from IssueToken>\"}" \
  localhost:8083 auth.v1.AuthService/RefreshToken
```

Returns a new access/refresh pair. The refresh token you just used is immediately invalidated — using it a second time returns `Unauthenticated`.

### `GetJWKS`

```bash
grpcurl -plaintext -import-path . -proto proto/auth/v1/auth.proto \
  -d '{}' localhost:8083 auth.v1.AuthService/GetJWKS
```

Returns the current RSA public key in JWK format. This is what a verifying service (the Client-Facing API) should fetch and cache — match a token's JWT header `kid` against the `kid` here.

### Verifying a token yourself

Tokens are RS256-signed JWTs with claims `sub` (client_id), `scope`, `iat`, `exp`, `iss`. Decode the payload without a library:

```bash
echo "<token>" | cut -d. -f2 | tr '_-' '/+' | base64 -d | jq .
```

## Running locally without Docker

```bash
go run ./cmd/server keygen          # once, if keys/private.pem doesn't exist yet
# apply migrations/001_create_clients.sql to your own Postgres instance
go run ./cmd/seed
go run ./cmd/server
```

## Testing

Unit tests (no Docker needed):

```bash
go test ./internal/client/... ./internal/token/... ./cmd/server/... -v
```

End-to-end smoke test against a running stack:

```bash
docker compose up --build -d
./scripts/smoke-test.sh
```

Run unit tests first — if those fail, the bug is in core logic and the smoke test will just produce confusing downstream failures on top of it.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `sql: unknown driver "pgx" (forgotten import?)` | The binary that calls `sql.Open("pgx", ...)` is missing `_ "github.com/jackc/pgx/v5/stdlib"` in its imports. Driver registration is per-binary — `cmd/seed` having it doesn't cover `cmd/server`. |
| `auth-db` never becomes healthy / exits immediately | `POSTGRES_USER`/`POSTGRES_PASSWORD`/`POSTGRES_DB` missing from `.env`, or not matching the credentials in `AUTH_DB_DSN`. |
| `expiresIn` is `0`, or every token looks instantly expired | TTL env vars parsed into `time.Duration` without multiplying by `time.Second` — treats "60" as 60 nanoseconds, not 60 seconds. |
| `AUTH_ENABLE_REFLECTION=true` doesn't seem to do anything | Check the flag is actually being assigned `true` in that branch, not re-assigned `false`. |
| Smoke test check 5 fails, showing what looks like an unchanged access token | Not a bug: RS256 signing is deterministic, and two token issuances landing in the same wall-clock second produce a byte-identical access token (same `iat`/`exp`). The refresh token (a random opaque value) is the reliable signal that rotation happened — compare that instead. |
| Changed `.env` but nothing changed in the running container | Compiled Go changes need `docker compose up --build -d`, not just `up -d`. Env-only changes (e.g. a TTL value) only need `up -d` but do need the container recreated, not just restarted. |

## Security notes

- Three independent trust domains: BNPB↔BMKG, BNPB↔PVMBG, and this service↔downstream clients. This service's signing key is never shared with, or derived from, the other two.
- Client secrets are bcrypt-hashed at rest; refresh tokens are SHA-256-hashed at rest (deliberately not bcrypt — they're already high-entropy random values, so a slow KDF buys nothing and blocks an indexed lookup).
- Unknown `client_id` and wrong secret are indistinguishable to the caller, including timing (a dummy bcrypt comparison runs on the "unknown client" path).
- Refresh tokens are single-use with rotation; reuse of a consumed token is rejected.
- No secrets are committed — `.env` and `keys/` are gitignored; `.env.example` holds placeholders only.
- gRPC reflection is off by default; enabling it exposes the full API surface to anyone who can reach the port.
- Transport is plaintext gRPC inside the Docker network for this milestone; only the gRPC port (`8083`) is published to the host, `auth-db` and the health port are not. mTLS is the stated next step for production.

## Project layout

```
cmd/
  server/     entrypoint — serves the gRPC API, or runs `keygen` as a subcommand
  seed/       one-shot: inserts the three client identities
internal/
  client/     client identities, bcrypt auth, Postgres repository
  token/      access token (JWT) + refresh token (hash, store, consume)
  keys/       signing key load/generate, JWKS
  server/     gRPC handlers (IssueToken, RefreshToken, GetJWKS) + interceptors
proto/        .proto source and generated code
migrations/   SQL schema
scripts/      smoke-test.sh
```