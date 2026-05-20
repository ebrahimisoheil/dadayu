# Containerization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Dagster (webserver + daemon + code server) as Docker Compose services backed by Postgres, replacing the local `dagster dev` workflow, while keeping local dev ergonomic via bind-mount overrides.

**Architecture:** Single Python image (built from existing `Dockerfile`) used by all three Dagster services with different commands. Dagster config files (`dagster.yaml`, `workspace.yaml`) baked into the image and seeded into a named volume at first run. `compose.override.yml` (git-ignored) adds source bind-mounts for local iteration without rebuilds.

**Tech Stack:** Docker Compose, Dagster 1.7, dagster-dbt, Postgres 16, ClickHouse 24, Python 3.12-slim

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Delete | `fetch_crypto_info.py` | Orphan script — superseded by Dagster asset |
| Delete | `fetch_crypto_prices.py` | Orphan script — superseded by Dagster asset |
| Delete | `fetch_hourly_prices.py` | Orphan script — superseded by Dagster asset |
| Delete | `fetch_ticker_info.py` | Orphan script — superseded by Dagster asset |
| Create | `dagster_config/dagster.yaml` | Postgres storage backend config |
| Create | `dagster_config/workspace.yaml` | gRPC code server location for webserver/daemon |
| Modify | `Dockerfile` | Copy dagster_config into image, set DAGSTER_HOME |
| Modify | `docker-compose.yml` | Add Postgres + 3 Dagster services + health checks + volumes |
| Create | `compose.override.yml` | Local dev bind mounts (git-ignored) |
| Modify | `.gitignore` | Ignore `compose.override.yml` |

---

### Task 1: Delete orphan scripts

These scripts pre-date the Dagster pipeline. Their logic lives in `dagster_pipeline/assets/`.

**Files:**
- Delete: `fetch_crypto_info.py`
- Delete: `fetch_crypto_prices.py`
- Delete: `fetch_hourly_prices.py`
- Delete: `fetch_ticker_info.py`

- [ ] **Step 1: Delete the files**

```bash
rm fetch_crypto_info.py fetch_crypto_prices.py fetch_hourly_prices.py fetch_ticker_info.py
```

- [ ] **Step 2: Verify deletion**

```bash
ls fetch_*.py 2>&1
```

Expected: `ls: cannot access 'fetch_*.py': No such file or directory` (or equivalent on macOS)

- [ ] **Step 3: Commit**

```bash
git add -u
git commit -m "chore: delete orphan fetch scripts superseded by dagster assets"
```

---

### Task 2: Create Dagster config files

`dagster.yaml` tells all Dagster services to use Postgres for run/event/schedule storage. `workspace.yaml` tells the webserver and daemon where the code server gRPC lives. Both are baked into the image — no secrets, safe to commit.

**Files:**
- Create: `dagster_config/dagster.yaml`
- Create: `dagster_config/workspace.yaml`

- [ ] **Step 1: Create `dagster_config/dagster.yaml`**

```yaml
storage:
  postgres:
    postgres_db:
      username: dagster
      password: dagster
      hostname: dadayu_postgres
      db_name: dagster
      port: 5432
```

- [ ] **Step 2: Create `dagster_config/workspace.yaml`**

```yaml
load_from:
  - grpc_server:
      host: dadayu_dagster_code
      port: 4000
      location_name: dadayu
```

- [ ] **Step 3: Commit**

```bash
git add dagster_config/
git commit -m "feat: dagster postgres storage config and workspace"
```

---

### Task 3: Update Dockerfile

Copy `dagster_config/` into the image at `/opt/dagster/home/` so that when Docker seeds the `dagster_home` named volume on first run, both config files are present. Set `DAGSTER_HOME` so all Dagster processes know where to look.

**Files:**
- Modify: `Dockerfile`

- [ ] **Step 1: Update `Dockerfile`**

Replace the full file with:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dagster_config/ /opt/dagster/home/
ENV DAGSTER_HOME=/opt/dagster/home

COPY . .

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 2: Verify build succeeds**

```bash
docker build -t dadayu:latest .
```

Expected: `Successfully tagged dadayu:latest` (or equivalent). No errors.

- [ ] **Step 3: Verify config files land in the image**

```bash
docker run --rm dadayu:latest ls /opt/dagster/home/
```

Expected output includes:
```
dagster.yaml
workspace.yaml
```

- [ ] **Step 4: Commit**

```bash
git add Dockerfile
git commit -m "feat: bake dagster config into image, set DAGSTER_HOME"
```

---

### Task 4: Add Postgres service and ClickHouse health check to docker-compose.yml

Dagster's `depends_on: condition: service_healthy` requires a health check on Postgres and ClickHouse. Add both. Add `dadayu_postgres` service and `dadayu_pg_data` volume.

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Write the updated `docker-compose.yml`** (services section — Postgres + ClickHouse health check only, Dagster services added in Task 5)

Replace the full `docker-compose.yml` with:

```yaml
services:
  dadayu_clickhouse:
    image: clickhouse/clickhouse-server:24
    container_name: dadayu_clickhouse
    ports:
      - "8123:8123"
      - "9000:9000"
    environment:
      CLICKHOUSE_DB: ${CLICKHOUSE_DB:-dadayu}
      CLICKHOUSE_USER: ${CLICKHOUSE_USER:-dadayu}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD:-changeme}
    volumes:
      - dadayu_ch_data:/var/lib/clickhouse
      - ./db/clickhouse_init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8123/ping || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 5

  dadayu_postgres:
    image: postgres:16-alpine
    container_name: dadayu_postgres
    environment:
      POSTGRES_USER: dagster
      POSTGRES_PASSWORD: dagster
      POSTGRES_DB: dagster
    volumes:
      - dadayu_pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U dagster"]
      interval: 10s
      timeout: 5s
      retries: 5

  dadayu_api:
    build: .
    image: dadayu:latest
    container_name: dadayu_api
    ports:
      - "8000:8000"
    env_file: .env
    environment:
      CLICKHOUSE_HOST: dadayu_clickhouse
    depends_on:
      dadayu_clickhouse:
        condition: service_healthy

  dadayu_dbt:
    build:
      context: .
      dockerfile: Dockerfile.dbt
    container_name: dadayu_dbt
    volumes:
      - ./warehouse:/usr/app/dbt
      - ~/.dbt:/root/.dbt:ro
    working_dir: /usr/app/dbt
    depends_on:
      dadayu_clickhouse:
        condition: service_healthy
    profiles:
      - tools

volumes:
  dadayu_ch_data:
  dadayu_pg_data:
```

- [ ] **Step 2: Validate compose syntax**

```bash
docker compose config --quiet
```

Expected: exits 0, no output (quiet mode suppresses valid config).

- [ ] **Step 3: Verify Postgres starts healthy**

```bash
docker compose up -d dadayu_postgres
docker compose ps dadayu_postgres
```

Expected: `dadayu_postgres` shows `healthy` status within ~30 seconds.

- [ ] **Step 4: Tear down**

```bash
docker compose down
```

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add postgres service and health checks to compose"
```

---

### Task 5: Add Dagster services to docker-compose.yml

Add three Dagster services: `dadayu_dagster_code` (gRPC code server), `dadayu_dagster_webserver` (UI), `dadayu_dagster_daemon` (schedules). All use `image: dadayu:latest` built by `dadayu_api`. Add `dagster_home` named volume.

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Write the final `docker-compose.yml`**

Replace the full file with:

```yaml
services:
  dadayu_clickhouse:
    image: clickhouse/clickhouse-server:24
    container_name: dadayu_clickhouse
    ports:
      - "8123:8123"
      - "9000:9000"
    environment:
      CLICKHOUSE_DB: ${CLICKHOUSE_DB:-dadayu}
      CLICKHOUSE_USER: ${CLICKHOUSE_USER:-dadayu}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD:-changeme}
    volumes:
      - dadayu_ch_data:/var/lib/clickhouse
      - ./db/clickhouse_init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8123/ping || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 5

  dadayu_postgres:
    image: postgres:16-alpine
    container_name: dadayu_postgres
    environment:
      POSTGRES_USER: dagster
      POSTGRES_PASSWORD: dagster
      POSTGRES_DB: dagster
    volumes:
      - dadayu_pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U dagster"]
      interval: 10s
      timeout: 5s
      retries: 5

  dadayu_dagster_code:
    image: dadayu:latest
    container_name: dadayu_dagster_code
    command: dagster code-server start -h 0.0.0.0 -p 4000 -m dagster_pipeline.definitions
    env_file: .env
    environment:
      DAGSTER_HOME: /opt/dagster/home
      CLICKHOUSE_HOST: dadayu_clickhouse
    volumes:
      - dagster_home:/opt/dagster/home
    depends_on:
      dadayu_clickhouse:
        condition: service_healthy
      dadayu_postgres:
        condition: service_healthy

  dadayu_dagster_webserver:
    image: dadayu:latest
    container_name: dadayu_dagster_webserver
    command: dagster-webserver -h 0.0.0.0 -p 3000
    ports:
      - "3000:3000"
    environment:
      DAGSTER_HOME: /opt/dagster/home
    volumes:
      - dagster_home:/opt/dagster/home
    depends_on:
      - dadayu_dagster_code

  dadayu_dagster_daemon:
    image: dadayu:latest
    container_name: dadayu_dagster_daemon
    command: dagster-daemon run
    environment:
      DAGSTER_HOME: /opt/dagster/home
    volumes:
      - dagster_home:/opt/dagster/home
    depends_on:
      - dadayu_dagster_code

  dadayu_api:
    build: .
    image: dadayu:latest
    container_name: dadayu_api
    ports:
      - "8000:8000"
    env_file: .env
    environment:
      CLICKHOUSE_HOST: dadayu_clickhouse
    depends_on:
      dadayu_clickhouse:
        condition: service_healthy

  dadayu_dbt:
    build:
      context: .
      dockerfile: Dockerfile.dbt
    container_name: dadayu_dbt
    volumes:
      - ./warehouse:/usr/app/dbt
      - ~/.dbt:/root/.dbt:ro
    working_dir: /usr/app/dbt
    depends_on:
      dadayu_clickhouse:
        condition: service_healthy
    profiles:
      - tools

volumes:
  dadayu_ch_data:
  dadayu_pg_data:
  dagster_home:
```

> **Note:** `dadayu_dagster_code` uses `image: dadayu:latest` without `build:` — it relies on the image built by `dadayu_api`. Always run `docker compose build` before `docker compose up` on a fresh clone or after code changes.

- [ ] **Step 2: Validate compose syntax**

```bash
docker compose config --quiet
```

Expected: exits 0, no errors.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add dagster webserver, daemon, and code server services"
```

---

### Task 6: Create compose.override.yml and update .gitignore

`compose.override.yml` is picked up automatically by `docker compose up` on local. It adds bind-mounts for source dirs into the code server so edits to `dagster_pipeline/`, `dadayu/`, or `warehouse/` take effect without a rebuild (code server auto-reloads). Git-ignore it so it never ships to prod.

**Files:**
- Create: `compose.override.yml`
- Modify: `.gitignore`

- [ ] **Step 1: Create `compose.override.yml`**

```yaml
services:
  dadayu_dagster_code:
    volumes:
      - dagster_home:/opt/dagster/home
      - ./dagster_pipeline:/app/dagster_pipeline
      - ./dadayu:/app/dadayu
      - ./warehouse:/app/warehouse
```

- [ ] **Step 2: Update `.gitignore`**

Add to the end of `.gitignore`:

```
compose.override.yml
```

- [ ] **Step 3: Verify override is ignored**

```bash
git status compose.override.yml
```

Expected: file not listed (ignored).

- [ ] **Step 4: Commit**

```bash
git add .gitignore
git commit -m "chore: add compose.override.yml for local dev bind mounts, gitignore it"
```

---

### Task 7: Full smoke test

Verify the entire stack starts, Dagster UI loads, and your assets are visible.

**Files:** None — verification only.

- [ ] **Step 1: Build the image**

```bash
docker compose build
```

Expected: exits 0. Image `dadayu:latest` built.

- [ ] **Step 2: Start all services**

```bash
docker compose up -d
```

Expected: all containers start without error.

- [ ] **Step 3: Check all services are running**

```bash
docker compose ps
```

Expected: all services show `running` (not `exited`). Give it ~30 seconds for health checks to pass.

- [ ] **Step 4: Check code server logs**

```bash
docker compose logs dadayu_dagster_code --tail 20
```

Expected: lines like:
```
Started Dagster code server for module dagster_pipeline.definitions on 0.0.0.0:4000
```
No `ModuleNotFoundError` or `ImportError`.

- [ ] **Step 5: Check daemon logs**

```bash
docker compose logs dadayu_dagster_daemon --tail 20
```

Expected: lines like:
```
dagster-daemon running
```
No `NoDaemonHeartbeats` errors after ~20 seconds.

- [ ] **Step 6: Open Dagster UI**

Open `http://localhost:3000` in browser.

Expected:
- Dagster UI loads
- `dadayu` code location visible in left nav
- Assets (equity_ohlcv, equity_ticker_info, crypto_ohlcv, crypto_info, dadayu_dbt_assets) visible in asset graph
- Jobs (equity_job, crypto_job) visible

- [ ] **Step 7: Verify FastAPI still works**

```bash
curl -s http://localhost:8000/docs | head -5
```

Expected: HTML response (Swagger UI).

- [ ] **Step 8: Tear down**

```bash
docker compose down
```

---

## Troubleshooting

**Code server fails with `ModuleNotFoundError: No module named 'dagster_pipeline'`**
The image wasn't rebuilt after code changes. Run `docker compose build` then `docker compose up -d`.

**Webserver shows "No code locations loaded"**
The code server hasn't finished starting. Check `docker compose logs dadayu_dagster_code`. If it's still initializing, wait 10–15 seconds and refresh.

**`dagster.yaml` changes not picked up**
The `dagster_home` named volume is seeded from the image only on first run (when the volume is empty). To pick up config changes: `docker compose down -v` (destroys volume — loses run history) then `docker compose up -d`. On prod, this is intentional: run history is preserved across deploys.

**ClickHouse health check failing on first run**
ClickHouse can take 30–60 seconds to initialize on a fresh volume. Dagster services will wait due to `depends_on: condition: service_healthy`. No action needed — they'll start once ClickHouse is ready.
