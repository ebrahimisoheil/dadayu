# Containerization Design — DADAYU AI

**Date:** 2026-05-20  
**Status:** Approved

## Goal

Containerize the full repo so `docker compose up` replaces `dagster dev` on local and deploys cleanly to a VPS. Dagster moves from SQLite (local `dagster_home/`) to Postgres-backed multi-service deployment.

## Services

| Service | Image | Purpose |
|---|---|---|
| `dadayu_clickhouse` | `clickhouse/clickhouse-server:24` | Market data storage — unchanged |
| `dadayu_postgres` | `postgres:16-alpine` | Dagster metadata (runs, events, schedules) |
| `dadayu_dagster_code` | built from `Dockerfile` | gRPC code server — serves assets/jobs/schedules |
| `dadayu_dagster_webserver` | same image, `dagster-webserver` cmd | Dagster UI on port 3000 |
| `dadayu_dagster_daemon` | same image, `dagster-daemon run` cmd | Executes schedules and sensors |
| `dadayu_api` | built from `Dockerfile` | FastAPI on port 8000 — unchanged |
| `dadayu_dbt` | built from `Dockerfile.dbt` | dbt CLI, tools profile — unchanged |

One image for all three Dagster services. `requirements.txt` already installs dagster, dagster-webserver, dagster-dbt, dbt-clickhouse. No new Dockerfile needed — services differ only by `command:`.

## Configuration

**`dagster_config/` directory** (checked into git, no secrets):

- `dagster_config/dagster.yaml` — Postgres storage backend, references env vars for credentials
- `dagster_config/workspace.yaml` — points webserver/daemon at `dadayu_dagster_code:4000` via gRPC

Both files copied into image at `/opt/dagster/home/` during build. `DAGSTER_HOME=/opt/dagster/home` set in Dockerfile. Named volume `dagster_home` mounts there — Docker seeds it from image on first run, persists runtime state across restarts.

**Secrets:**

- Postgres credentials hardcoded in compose (`dagster`/`dagster`/`dagster`) — internal network only, never exposed
- `.env` unchanged — ClickHouse creds and API keys only
- Code server gets `env_file: .env` so assets reach ClickHouse

## Local Dev vs Prod

**Prod:** `docker compose -f docker-compose.yml up`

**Local:** `docker compose up` — automatically picks up `compose.override.yml` which bind-mounts source dirs into `dadayu_dagster_code`:

```
./dagster_pipeline  →  /app/dagster_pipeline
./dadayu            →  /app/dadayu
./warehouse         →  /app/warehouse
```

No rebuild needed when iterating on code or dbt models locally. `compose.override.yml` is git-ignored.

## Startup Order & Health Checks

```
dadayu_postgres (healthy) ────┐
dadayu_clickhouse (healthy) ──┴──► dadayu_dagster_code ──► dadayu_dagster_webserver
                                                        └──► dadayu_dagster_daemon
```

Health checks:
- Postgres: `pg_isready -U dagster`
- ClickHouse: `wget -qO- http://localhost:8123/ping`
- Code server: TCP port 4000 (webserver/daemon use `depends_on: condition: service_started`)

## Volumes

| Volume | Purpose |
|---|---|
| `dadayu_ch_data` | ClickHouse data — existing |
| `dadayu_pg_data` | Postgres data — new |
| `dagster_home` | Dagster runtime state (compute logs, artifacts) — new |

## Ports

| Port | Service |
|---|---|
| 3000 | Dagster webserver |
| 8000 | FastAPI |
| 8123 | ClickHouse HTTP |
| 9000 | ClickHouse native |

Postgres port not exposed outside Docker network.

## File Changes

**New:**
- `dagster_config/dagster.yaml`
- `dagster_config/workspace.yaml`
- `compose.override.yml` (git-ignored)

**Modified:**
- `docker-compose.yml` — add postgres + 3 Dagster services + health checks + new volumes
- `Dockerfile` — add dagster_config copy + `ENV DAGSTER_HOME`
- `.gitignore` — add `compose.override.yml`

**Deleted (orphan scripts superseded by Dagster pipeline):**
- `fetch_crypto_info.py`
- `fetch_crypto_prices.py`
- `fetch_hourly_prices.py`
- `fetch_ticker_info.py`

**Untouched:**
- `Dockerfile.dbt`, `dadayu/`, `dagster_pipeline/`, `warehouse/`, `tests/`, `api.py`

## Migration Note

Existing `dagster_home/` (SQLite-backed) stays on local disk untouched. New setup starts fresh with Postgres. No run history migration — not worth the effort.
