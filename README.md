# DADAYU AI — Financial Data Pipeline

A production-grade financial data pipeline that ingests equity, cryptocurrency, and prediction market data into ClickHouse, transforms it through dbt, and orchestrates everything with Dagster.

---

## What It Does

| Data Source | Coverage | Cadence |
|---|---|---|
| Yahoo Finance (equity) | 1,089 tickers across US, Germany, India markets | Daily at 22:00 UTC weekdays |
| Yahoo Finance (crypto) | 18 crypto pairs (BTC, ETH, SOL, …) | Every 4 hours |
| Polymarket (prediction markets) | 100 active markets, 90-day history | Every 4 hours |

Outputs: clean OHLCV marts, technical indicator signals (RSI, MACD, Bollinger Bands, ATR), and Polymarket probability signals — all queryable from ClickHouse.

---

## Architecture

```
Yahoo Finance API ──► equity_ohlcv  ──────┐
                ──► equity_ticker_info ──►│
                                          │  ClickHouse  ──► dbt transform ──► marts
CoinGecko API  ──► crypto_info ──────────►│  (raw tables)     (incremental)
Yahoo Finance  ──► crypto_ohlcv ─────────►│
                                          │
Gamma API      ──► polymarket_markets ───►│
CLOB API       ──► polymarket_prices ────►│
```

**Stack:**
- **Storage:** ClickHouse 24 (columnar, MergeTree family)
- **Orchestration:** Dagster 1.7 with webserver, daemon, and code server
- **Transformation:** dbt-core 1.8 with clickhouse adapter
- **Metadata store:** PostgreSQL 16 (Dagster run history)
- **API:** FastAPI (optional, for triggering runs via HTTP)
- **Monitoring:** Elementary Data (dbt test result tracking)

---

## Project Structure

```
.
├── dadayu/                     # Core Python library
│   ├── db.py                   # ClickHouse connection factory
│   ├── insert.py               # Bulk insert helpers
│   ├── watermark.py            # Per-table/per-market incremental watermarks
│   └── ingest/
│       ├── equity.py           # Yahoo Finance equity + ticker info
│       ├── crypto.py           # Yahoo Finance crypto OHLCV + CoinGecko metadata
│       └── polymarket.py       # Gamma API (markets) + CLOB API (prices)
│
├── dagster_pipeline/           # Dagster orchestration
│   ├── assets/
│   │   ├── equity.py           # equity_ohlcv, equity_ticker_info assets
│   │   ├── crypto.py           # crypto_ohlcv, crypto_info assets
│   │   ├── polymarket.py       # polymarket_markets, polymarket_prices assets
│   │   └── dbt_assets.py       # dbt run wired as Dagster asset
│   ├── resources.py            # ClickhouseResource
│   ├── schedules.py            # equity_job, crypto_job, polymarket_job + schedules
│   └── definitions.py          # Dagster Definitions entry point
│
├── warehouse/                  # dbt project
│   ├── models/
│   │   ├── staging/            # Clean raw tables (incremental or table)
│   │   │   ├── yahoo/          # stg_yahoo__ohlcv_{1h,4h,1d}, stg_yahoo__crypto_ohlcv_*, stg_yahoo__ticker_info
│   │   │   ├── coingecko/      # stg_coingecko__crypto_info
│   │   │   └── polymarket/     # stg_polymarket__markets, stg_polymarket__prices
│   │   ├── intermediate/       # Joins + sessionization (incremental)
│   │   │   ├── int_equity_ohlcv_{1h,4h,1d}
│   │   │   ├── int_crypto_ohlcv_{1h,4h,1d}
│   │   │   ├── int_calendar_sessions
│   │   │   └── int_polymarket_prices_{1h,1d}
│   │   └── marts/
│   │       ├── markets/        # fct_ohlcv_{1h,4h,1d}, fct_ohlcv_crypto_{1h,4h,1d}, fct_polymarket_signals
│   │       ├── indicators/     # fct_indicators_{1h,4h,1d}, fct_indicators_crypto_{1h,4h,1d}
│   │       └── reference/      # dim_calendar, dim_equity_symbol, dim_crypto_symbol
│   ├── snapshots/              # SCD Type 2 for market metadata
│   │   ├── snap_dim_equity_symbol.sql
│   │   ├── snap_dim_crypto_symbol.sql
│   │   └── snap_dim_polymarket_market.sql
│   ├── seeds/
│   │   ├── crypto_universe.csv         # 18 tracked crypto pairs
│   │   ├── trading_calendar.csv        # Market open/close times by exchange
│   │   ├── gics_hierarchy.csv          # GICS sector/industry taxonomy
│   │   └── polymarket_asset_map.csv    # Manual ticker overrides for Polymarket markets
│   └── macros/
│       └── indicators/         # SMA, EMA, RSI, MACD, ATR, Bollinger Band macros
│
├── db/
│   └── clickhouse_init.sql     # Table DDL (runs on first container start)
│
├── scripts/
│   └── check_data_quality.py   # Ad-hoc DQ checks (38 checks, ~5s runtime)
│
├── tests/                      # pytest unit tests
│   ├── test_polymarket_ingest.py
│   ├── test_watermark.py
│   └── test_dagster_assets.py
│
├── docker-compose.yml          # All services
├── compose.override.yml        # Dev: live-mount source code into containers
├── Dockerfile                  # App image (dagster + dbt + dadayu)
├── Dockerfile.dbt              # Standalone dbt container (tools profile)
└── requirements.txt
```

---

## Quick Start

### Prerequisites

- Docker Desktop with at least 8 GB RAM allocated
- Python 3.12 (for local dev/tests only)
- A `~/.dbt/profiles.yml` with a `dadayu` profile pointing at `dadayu_clickhouse:8123`

### 1. Configure environment

```bash
cp .env.example .env   # edit if needed — defaults work for local Docker
```

Default `.env` values:
```
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_DB=dadayu
CLICKHOUSE_USER=dadayu
CLICKHOUSE_PASSWORD=changeme
DAGSTER_PG_USER=dagster
DAGSTER_PG_PASSWORD=dagster
DAGSTER_PG_DB=dagster
```

### 2. Start all services

```bash
docker compose up -d
```

Services started:
| Service | Port | Description |
|---|---|---|
| `dadayu_clickhouse` | 8123, 9000 | ClickHouse database |
| `dadayu_postgres` | — | Dagster + Metabase metadata store |
| `dadayu_postgres_setup` | — | One-shot init: creates `metabase` database, exits |
| `dadayu_dagster_code` | 4000 | Dagster code server |
| `dadayu_dagster_webserver` | 3000 | Dagster UI |
| `dadayu_dagster_daemon` | — | Schedule runner |
| `dadayu_api` | 8000 | FastAPI trigger endpoint |
| `dadayu_metabase` | 3001 | Metabase BI (ClickHouse built-in as of v0.54) |

### 3. Open Dagster UI

`http://localhost:3000`

### 4. Open Metabase

`http://localhost:3001`

First launch: Metabase runs a setup wizard (~60s to start). Connect to ClickHouse:

| Field | Value |
|---|---|
| Database type | ClickHouse |
| Host | `dadayu_clickhouse` |
| Port | `8123` |
| Database | `dadayu` |
| Username | `dadayu` |
| Password | `changeme`|

Materialize assets manually or wait for the first scheduled run.

### 4. Run dbt transforms

```bash
# Inside the dagster code container (has correct ClickHouse host)
docker exec dadayu_dagster_code bash -c \
  "cd /app/warehouse && dbt run --profiles-dir /root/.dbt"
```

---

## Data Model

### Raw ClickHouse Tables

| Table | Engine | Key | Description |
|---|---|---|---|
| `prices_hourly` | ReplacingMergeTree | (ticker, market, datetime) | Raw equity 1h bars |
| `prices_4h` | ReplacingMergeTree | (ticker, market, datetime) | Raw equity 4h bars |
| `prices_daily` | ReplacingMergeTree | (ticker, market, date) | Raw equity daily bars |
| `crypto_prices_hourly` | ReplacingMergeTree | (ticker, market, datetime) | Raw crypto 1h bars |
| `crypto_prices_4h` | ReplacingMergeTree | (ticker, market, datetime) | Raw crypto 4h bars |
| `crypto_metadata` | ReplacingMergeTree | coin_id | CoinGecko metadata |
| `tickers` | ReplacingMergeTree | (ticker, market) | Equity ticker info |
| `polymarket_markets` | ReplacingMergeTree(fetched_at) | condition_id | Market metadata (daily) |
| `polymarket_prices` | ReplacingMergeTree(ingested_at) | (condition_id, ts) | Hourly probability snapshots |

All raw tables use `FINAL` at query time in staging models for deduplication.

### dbt Mart Tables

#### `fct_ohlcv_{1h,4h,1d}` — Equity OHLCV
| Column | Type | Description |
|---|---|---|
| ticker, market, ts | key | Bar identifier |
| open, high, low, close | Float64 | OHLC prices |
| volume | UInt64 | Volume traded |
| return_pct | Float64 | Bar-over-bar return (null on first bar per ticker) |
| session_id, is_trading_day | — | Calendar enrichment |
| name, sector, industry | — | Company metadata from GICS |

#### `fct_ohlcv_crypto_{1h,4h,1d}` — Crypto OHLCV
Same as equity OHLCV plus `category`, `market_rank` from CoinGecko.

#### `fct_indicators_{1h,4h,1d}` — Equity Technical Indicators
| Column | Description |
|---|---|
| sma_20 | 20-period Simple Moving Average |
| ema_20 | 20-period Exponential Moving Average |
| rsi_14 | 14-period RSI (null on first 14 bars) |
| macd_line, macd_signal, macd_hist | MACD (12/26/9) |
| atr_14 | 14-period Average True Range |
| bb_upper, bb_middle, bb_lower | Bollinger Bands (20, 2σ) |

#### `fct_polymarket_signals` — Prediction Market Signals
| Column | Type | Description |
|---|---|---|
| condition_id, ts | key | Market + hour bucket |
| probability | Float64 | Close-of-hour YES probability [0, 1] |
| prob_change | Float64 | Δp vs previous hour — primary signal |
| log_odds | Float64 | ln(p/(1-p)), clipped to p∈[0.01, 0.99] |
| volume_usd | Float64 | USD volume in this bucket |
| linked_asset | String | Correlated ticker (e.g. `BTC-USD`) — null if no clear link |
| asset_type | String | `crypto` / `equity` / `macro` / null |
| days_to_resolution | Int32 | Days until market closes. Filter `> 2` to exclude expiry noise. |
| is_interpolated | Bool | Always false currently. Reserved for forward-fill flag. |

**Correlation join (future):** `fct_polymarket_signals` LEFT JOIN `fct_ohlcv_crypto_1h` ON `linked_asset = ticker AND ts = ts`.

---

## Dagster Jobs & Schedules

| Job | Assets | Schedule | Trigger |
|---|---|---|---|
| `equity_job` | equity_ohlcv + equity_ticker_info + dbt | `0 22 * * 1-5` | Daily 22:00 UTC weekdays |
| `crypto_job` | crypto_ohlcv + crypto_info + dbt | `0 */4 * * *` | Every 4 hours |
| `polymarket_job` | polymarket_markets + polymarket_prices | `0 */4 * * *` | Every 4 hours |

dbt runs inside `equity_job` and `crypto_job`. `polymarket_job` intentionally excludes dbt — signals mart is rebuilt when equity_job or crypto_job next runs.

---

## Ingestion Details

### Equity (Yahoo Finance)

- **OHLCV:** Fetched via `yfinance` for all tickers in `tickers` table. Watermarked per `(ticker, market, interval)` — only new bars fetched.
- **Ticker info:** Name, sector, industry, market cap, P/E ratio from Yahoo Finance ticker metadata.
- **Markets:** `us` (NYSE/NASDAQ), `germany` (XETRA), `india` (NSE/BSE).

### Crypto (Yahoo Finance + CoinGecko)

- **OHLCV:** Same yfinance pipeline as equity. 18 pairs (BTC-USD, ETH-USD, SOL-USD, …).
- **Metadata:** CoinGecko API for rank, category, chain. No API key required for read-only.

### Polymarket

- **Market discovery:** Gamma API — active markets with volume > $50k USD. Runs daily. Tombstones markets that disappear from API (sets `closed=True`).
- **Price history:** CLOB API — hourly probability snapshots per YES token. Per-market watermark, 90-day backfill on first run. Chunked into ≤15-day windows (API limit). Rate-limited at 0.15s between calls.
- **Linked asset detection:** Auto-parses market question text for crypto tickers using word-boundary regex. Manual overrides via `warehouse/seeds/polymarket_asset_map.csv`.

---

## dbt Materialization Strategy

| Layer | Strategy | Rationale |
|---|---|---|
| Staging OHLCV | `incremental` (delete+insert) | Only process new bars |
| Staging metadata | `table` | Small reference data, full refresh trivial |
| Intermediate OHLCV | `incremental` (delete+insert) | Only join new bars |
| Intermediate calendar | `table` | Static |
| Mart fct OHLCV | `table` | `lagInFrame` window function needs full history |
| Mart indicators | `table` | EMA/RSI recursive windows need full history |
| Mart dims | `table` | Small reference |
| Mart polymarket signals | `table` | `lagInFrame` window function |
| Snapshots | SCD Type 2 (`check` strategy) | Tracks metadata changes over time |

Indicator marts use `pre_hook="SET max_bytes_before_external_sort = 2000000000"` to allow disk spill on memory-constrained Docker setups.

---

## SCD Type 2 Snapshots

Three `check`-strategy snapshots track lifecycle changes:

| Snapshot | Unique Key | Tracked Columns |
|---|---|---|
| `snap_dim_equity_symbol` | (ticker, market) | name, sector, industry, market_cap |
| `snap_dim_crypto_symbol` | coin_id | market_rank, category |
| `snap_dim_polymarket_market` | condition_id | active, closed, outcome, linked_asset, asset_type |

Retains history when markets resolve, tickers delist, or crypto rankings change.

---

## dbt Packages

| Package | Use |
|---|---|
| `dbt-labs/dbt_utils` | `unique_combination_of_columns`, `generate_surrogate_key` |
| `calogica/dbt_expectations` | Available for range/regex tests (not yet fully deployed) |
| `elementary-data/elementary` | Test result tracking, source freshness monitoring |

---

## Data Quality

Run the ad-hoc quality check script:

```bash
python scripts/check_data_quality.py
```

38 checks across all datasets in ~5 seconds. Exits 0 on PASS/WARN, exits 1 on any FAIL.

**Check categories:**
- OHLCV integrity: `high >= low`, `close ∈ [low, high]`, no zero/negative prices, no duplicates
- Freshness: hours since last bar (warns at >26h equity, >5h crypto/polymarket)
- Extreme moves: >50% single-hour equity return, >30% crypto, >0.5 Δp Polymarket
- Cross-layer consistency: staging == intermediate == mart row counts
- Mart sanity: RSI ∈ [0, 100], log_odds finite, no unexpected nulls

**Current baseline (as of last run):** 38 PASS, 3 WARN (zero-volume bars on holidays, null RSI on first 14 bars, one VEDL.NS corporate action), 0 FAIL.

---

## Development

### Running tests

```bash
pytest tests/ -v
```

15 unit tests covering watermark logic, Polymarket ingestion, and Dagster asset definitions.

### Local dbt development

```bash
# Requires ~/.dbt/profiles.yml with dadayu profile pointing to dadayu_clickhouse:8123
docker exec dadayu_dagster_code bash -c "cd /app/warehouse && dbt run --profiles-dir /root/.dbt"
docker exec dadayu_dagster_code bash -c "cd /app/warehouse && dbt test --profiles-dir /root/.dbt"
```

### Live code reloading

`compose.override.yml` mounts `./dagster_pipeline`, `./dadayu`, and `./warehouse` into the code server container. Code changes are picked up without rebuilding the image.

### Adding a new equity ticker

Add the ticker to the `tickers` ClickHouse table (or seed). It will be picked up on the next `equity_job` run.

### Adding a Polymarket asset override

Edit `warehouse/seeds/polymarket_asset_map.csv`:
```csv
condition_id,linked_asset,asset_type
0xabc...,BTC-USD,crypto
```

Run `dbt seed` then re-materialize `polymarket_markets`.

---

## Polymarket API Notes

| API | Base URL | Auth | Limit |
|---|---|---|---|
| Gamma (market discovery) | `https://gamma-api.polymarket.com` | None | ~6 req/s |
| CLOB (price history) | `https://clob.polymarket.com` | None | 15-day max window per request |

Key gotcha: the CLOB `/prices-history` endpoint requires the **YES token ID** (`clobTokenIds[0]` from Gamma response), not the `conditionId`. These are different fields.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse hostname (use `dadayu_clickhouse` inside Docker) |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port |
| `CLICKHOUSE_DB` | `dadayu` | Database name |
| `CLICKHOUSE_USER` | `dadayu` | Username |
| `CLICKHOUSE_PASSWORD` | `changeme` | Password |
| `DAGSTER_PG_HOST` | `dadayu_postgres` | Postgres host for Dagster metadata |
| `DAGSTER_PG_USER` | `dagster` | Postgres user |
| `DAGSTER_PG_PASSWORD` | `dagster` | Postgres password |
| `DAGSTER_PG_DB` | `dagster` | Postgres database |
| `DAGSTER_HOME` | `/opt/dagster/home` | Dagster home directory (inside container) |
