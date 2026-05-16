# dbt Warehouse Design — Equity MVP

**Date:** 2026-05-16
**Status:** Approved
**Scope:** Equity market data only (Germany, US, India). Polymarket, crypto, orderbooks deferred.

---

## 1. Context

ClickHouse already contains:
- `prices_hourly` — 156,908 rows, 1h OHLCV, April 2026
- `prices_4h` — 67,354 rows, 4h OHLCV, April 2026
- `prices_daily` — 22,281 rows, daily OHLCV, April 2026

Missing: `tickers` table (equity metadata). Must be ingested before dbt runs.

---

## 2. Ingestion Addition (Pre-dbt)

### fetch_ticker_info.py (new script)

Calls `yfinance.Ticker(t).info` for all active tickers across 3 markets. **Appends** to ClickHouse `tickers` table — never truncates. ClickHouse `ReplacingMergeTree(fetched_at)` handles lazy dedup; staging queries with `FINAL`.

**ClickHouse table: `tickers`**

```sql
CREATE TABLE IF NOT EXISTS tickers (
    ticker       String,
    market       LowCardinality(String),
    name         String,
    sector       String,
    industry     String,
    currency     LowCardinality(String),
    country      String,
    market_cap   Nullable(Float64),
    pe_ratio     Nullable(Float64),
    fetched_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (market, ticker)
PARTITION BY market;
```

New API endpoint: `POST /run/fetch-ticker-info?market=all`

---

## 3. Project Structure

```
warehouse/
├── dbt_project.yml
├── packages.yml
├── profiles.yml.example
├── seeds/
│   ├── trading_calendar.csv
│   └── gics_hierarchy.csv
├── macros/
│   ├── time_bucket.sql
│   ├── ch_table_engine.sql
│   └── indicators/
│       ├── sma.sql
│       ├── ema.sql
│       ├── rsi.sql
│       ├── macd.sql
│       ├── atr.sql
│       └── bbands.sql
├── snapshots/
│   └── snap_dim_equity_symbol.sql
└── models/
    ├── staging/
    │   └── yahoo/
    │       ├── _sources.yml
    │       ├── stg_yahoo__ohlcv_1h.sql
    │       ├── stg_yahoo__ohlcv_4h.sql
    │       ├── stg_yahoo__ohlcv_1d.sql
    │       └── stg_yahoo__ticker_info.sql
    ├── intermediate/
    │   ├── int_calendar_sessions.sql
    │   ├── int_equity_ohlcv_1h.sql
    │   ├── int_equity_ohlcv_4h.sql
    │   └── int_equity_ohlcv_1d.sql
    └── marts/
        ├── reference/
        │   ├── dim_calendar.sql
        │   └── dim_equity_symbol.sql
        ├── markets/
        │   ├── fct_ohlcv_1h.sql
        │   ├── fct_ohlcv_4h.sql
        │   └── fct_ohlcv_1d.sql
        └── indicators/
            ├── fct_indicators_1h.sql
            ├── fct_indicators_4h.sql
            └── fct_indicators_1d.sql
```

**Deferred (stubbed `+enabled: false`):** `models/staging/polymarket/`, `models/staging/crypto/`, `models/marts/analytics/`, `models/marts/markets/fct_trades.sql`, `models/marts/markets/fct_ohlcv_1m.sql`

---

## 4. Seeds

### trading_calendar.csv
Columns: `date`, `market`, `is_trading_day`, `session_open_utc`, `session_close_utc`
One row per (date, market). Covers April 2026 initially; extended as data grows.

### gics_hierarchy.csv
Columns: `sector_id`, `sector_name`, `industry_group_id`, `industry_group_name`, `industry_id`, `industry_name`, `sub_industry_id`, `sub_industry_name`
Static GICS reference used to enrich `dim_equity_symbol` via join on sector name from yfinance.

---

## 5. Macros

### time_bucket.sql
Wraps ClickHouse `toStartOfInterval(dt, INTERVAL N unit)`. Usage: `{{ time_bucket('datetime', '4 hour') }}`.

### ch_table_engine.sql
Returns consistent `ENGINE = ReplacingMergeTree(...) ORDER BY (...) PARTITION BY (...)` clause based on args. All materialized models use this macro.

### indicators/
Each macro takes column expression(s) + window parameter, returns a SQL window expression.

| Macro | Inputs | Output |
|-------|--------|--------|
| `sma(col, n)` | close, period | rolling avg over n rows |
| `ema(col, n)` | close, period | exponential moving avg, multiplier `2/(n+1)` |
| `rsi(col, n)` | close, period | 0–100 momentum oscillator |
| `macd(col, fast, slow, signal)` | close, 3 periods | macd_line, signal_line, histogram |
| `atr(high, low, close, n)` | OHLC, period | average true range |
| `bbands(col, n, std_dev)` | close, period, multiplier | upper, middle, lower bands |

---

## 6. Staging Layer

**Materialization:** `view`. No logic, no joins — rename and cast only.

### _sources.yml
Declares 4 ClickHouse sources: `prices_hourly`, `prices_4h`, `prices_daily`, `tickers`.
Freshness SLAs: `prices_daily` warn after 2 days, error after 4 days.

### stg_yahoo__ohlcv_1h/4h/1d.sql
Selects from source table. Renames: `datetime/date → ts`, standardizes column names. Casts `volume` to `UInt64`. Drops `ingested_at`.

### stg_yahoo__ticker_info.sql
Selects from `tickers FINAL` (ClickHouse dedup keyword). Renames columns. Casts `market_cap`, `pe_ratio` as nullable floats.

---

## 7. Intermediate Layer

**Materialization:** `view`.

### int_calendar_sessions.sql
Reads `trading_calendar` seed. One row per `(date, market)` with session metadata. Canonical session lookup for all downstream joins.

### int_equity_ohlcv_1h/4h/1d.sql
Takes staged OHLCV. Left-joins `int_calendar_sessions` on `(date(ts), market)`. Adds: `session_id` (e.g. `US_20260401`), `is_trading_day`, `session_open_utc`, `session_close_utc`.

**Note:** `bar_number_in_session` and `is_first_bar_of_session` deferred to v2 (relevant for session-boundary indicators like VWAP reset).

---

## 8. Snapshots

### snap_dim_equity_symbol.sql
```sql
{% snapshot snap_dim_equity_symbol %}
  {{ config(
      target_schema='snapshots',
      unique_key=dbt_utils.generate_surrogate_key(['ticker', 'market']),
      strategy='check',
      check_cols=['name', 'sector', 'industry', 'market_cap']
  ) }}
  SELECT
    {{ dbt_utils.generate_surrogate_key(['ticker', 'market']) }} AS equity_id,
    *
  FROM {{ ref('stg_yahoo__ticker_info') }}
{% endsnapshot %}
```
Runs after each `fetch_ticker_info` ingestion. Captures metadata changes as SCD2 history.

---

## 9. Marts Layer

**Materialization:** `table` for reference dims, `view` for facts and indicators (flip to incremental when history > ~6 months).

### reference/dim_calendar.sql
Reads `int_calendar_sessions`. Adds derived columns: `year`, `month`, `week_of_year`, `day_of_week`. Materialized as `table`.

### reference/dim_equity_symbol.sql
Reads `snap_dim_equity_symbol WHERE dbt_valid_to IS NULL`. Left-joins `gics_hierarchy` seed on sector name for enriched GICS columns. Exposes current state only. Materialized as `view` (snapshot is the table).

### markets/fct_ohlcv_1h/4h/1d.sql
Reads from `int_equity_ohlcv_*`. Left-joins `dim_equity_symbol` on `(ticker, market)` for `name`, `sector`, `industry`. Adds `return_pct = (close - lag(close)) / lag(close)` via window function partitioned by `(ticker, market)` ordered by `ts`.

### indicators/fct_indicators_1h/4h/1d.sql
Reads from corresponding `fct_ohlcv_*`. Applies all 6 indicator macros. One row per `(ticker, market, ts)`.

**Output columns:** `ticker`, `market`, `ts`, `close`, `sma_20`, `ema_20`, `rsi_14`, `macd_line`, `macd_signal`, `macd_hist`, `atr_14`, `bb_upper`, `bb_middle`, `bb_lower`.

---

## 10. Project Config

### packages.yml
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0"]
  - package: calogica/dbt_expectations
    version: [">=0.10.0"]
  - package: elementary-data/elementary
    version: [">=0.15.0"]
```

### dbt_project.yml (materialization config)
```yaml
name: dadayu_warehouse
profile: dadayu

models:
  dadayu_warehouse:
    staging:      { +materialized: view }
    intermediate: { +materialized: view }
    marts:
      reference:  { +materialized: table }
      markets:    { +materialized: view }
      indicators: { +materialized: view }
      analytics:  { +enabled: false }

seeds:
  dadayu_warehouse:
    trading_calendar: { +column_types: { is_trading_day: UInt8 } }
```

### Docker
New `dadayu_dbt` service in `docker-compose.yml`. Mounts `./warehouse`. One-shot usage:
```bash
docker compose run dadayu_dbt dbt run
docker compose run dadayu_dbt dbt snapshot
docker compose run dadayu_dbt dbt test
```

### profiles.yml.example
ClickHouse connection: host `dadayu_clickhouse`, port `8123`, database `dadayu`. Committed as `.example`; actual `profiles.yml` lives in `~/.dbt/` (not committed). The `dadayu_dbt` Docker service mounts `~/.dbt/` into the container at `/root/.dbt/` so credentials stay out of the repo.

---

## 11. Execution Order

1. `fetch_ticker_info.py` — ingest metadata
2. `dbt seed` — load trading_calendar + gics_hierarchy
3. `dbt snapshot` — SCD2 on equity metadata
4. `dbt run` — staging → intermediate → marts
5. `dbt test` — data quality checks

---

## 12. Deferred (v2+)

- `bar_number_in_session`, `is_first_bar_of_session` on sub-daily intervals
- Flip `fct_*` from view → incremental table when history > 6 months
- Polymarket staging + intermediate + dims
- Crypto staging + intermediate + dims
- `map_pm_to_underlying` moat table
- `obt_unified_wide` cross-asset analytics table
- `fct_ohlcv_1m`, `fct_trades`, `fct_orderbook_top`
