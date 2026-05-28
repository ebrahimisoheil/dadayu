# Macro Regime & Cross-Asset Analytics — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest 26 macro/cross-asset tickers (ETFs, futures, rate indices) via yfinance and replace the single-factor S&P regime with a composite 6-dimension macro regime score (0–100 continuous + 5-state label).

**Architecture:** New `macro_prices_daily` Postgres table feeds a dedicated yfinance ingest asset → dbt staging → per-ticker indicator intermediate → regime pivot → Metabase mart. The old `int_market_regime_daily` becomes a passthrough `SELECT * FROM int_macro_regime_daily` so all downstream models keep working unchanged.

**Tech Stack:** Python 3.12, yfinance (via existing `download_ohlcv`), Postgres 16, Dagster 1.x, dbt-postgres, pytest with `unittest.mock`.

**Revised:** 2026-05-28 — updated for Postgres (migrated from ClickHouse).

---

## File Map

| Status | Path | Responsibility |
|--------|------|----------------|
| NEW | `warehouse/seeds/macro_universe.csv` | 26-ticker reference table |
| MOD | `warehouse/dbt_project.yml` | seed column_types for macro_universe |
| MOD | `db/postgres_init.sql` | `macro_prices_daily` DDL |
| NEW | `dadayu/ingest/macro.py` | load_symbols / load_universe / MARKET / INTERVAL_TABLE |
| NEW | `dagster_pipeline/assets/macro.py` | `macro_ohlcv` Dagster asset |
| MOD | `dagster_pipeline/assets/__init__.py` | export macro_ohlcv |
| MOD | `dagster_pipeline/schedules.py` | macro_job + macro_schedule (22:15 UTC weekdays) |
| MOD | `dagster_pipeline/definitions.py` | register macro asset + schedule |
| MOD | `tests/test_dagster_assets.py` | macro_ohlcv smoke test |
| MOD | `warehouse/models/01_staging/yahoo/_sources.yml` | add macro_prices_daily source |
| NEW | `warehouse/models/01_staging/yahoo/stg_yahoo__macro_ohlcv_daily.sql` | staging with seed join |
| NEW | `warehouse/models/02_intermediate/macro/int_macro_assets_daily.sql` | per-ticker indicators |
| NEW | `warehouse/models/02_intermediate/market/int_macro_regime_daily.sql` | 6-score composite regime |
| MOD | `warehouse/models/02_intermediate/market/int_market_regime_daily.sql` | passthrough → int_macro_regime_daily |
| MOD | `warehouse/models/02_intermediate/schema.yml` | tests for new intermediate models |
| NEW | `warehouse/models/03_marts/macro/mart_macro_regime_daily.sql` | Metabase-facing mart |
| NEW | `warehouse/models/03_marts/macro/schema.yml` | mart tests |

---

## Task 1: Seed + Postgres DDL

**Files:**
- Create: `warehouse/seeds/macro_universe.csv`
- Modify: `warehouse/dbt_project.yml`
- Modify: `db/postgres_init.sql`

- [ ] **Step 1: Create macro_universe.csv**

```
warehouse/seeds/macro_universe.csv
```

```csv
macro_id,ticker,market,name,instrument_type,regime_dimension
HYG,HYG,macro,iShares HY Corporate Bond,etf,credit
LQD,LQD,macro,iShares IG Corporate Bond,etf,credit
TLT,TLT,macro,iShares 20+ Year Treasury,etf,rates
IEF,IEF,macro,iShares 7-10 Year Treasury,etf,rates
SHY,SHY,macro,iShares 1-3 Year Treasury,etf,rates
TIP,TIP,macro,iShares TIPS Bond,etf,inflation
TNX,^TNX,macro,10-Year Treasury Yield,rate_index,rates
GLD,GLD,macro,SPDR Gold Shares,etf,inflation
SLV,SLV,macro,iShares Silver Trust,etf,inflation
USO,USO,macro,United States Oil Fund,etf,inflation
GCF,GC=F,macro,Gold Futures,future,inflation
SIF,SI=F,macro,Silver Futures,future,inflation
CLF,CL=F,macro,Crude Oil Futures,future,inflation
CPER,CPER,macro,United States Copper Index,etf,growth
DBB,DBB,macro,Invesco DB Base Metals,etf,growth
HGF,HG=F,macro,Copper Futures,future,growth
EFA,EFA,macro,iShares MSCI EAFE,etf,growth
EEM,EEM,macro,iShares MSCI Emerging Markets,etf,growth
UUP,UUP,macro,Invesco DB US Dollar Index,etf,dollar
VNQ,VNQ,macro,Vanguard Real Estate ETF,etf,rates
XLE,XLE,macro,Energy Select Sector SPDR,etf,sector
XLF,XLF,macro,Financial Select Sector SPDR,etf,sector
XLK,XLK,macro,Technology Select Sector SPDR,etf,sector
XLV,XLV,macro,Health Care Select Sector SPDR,etf,sector
XLI,XLI,macro,Industrial Select Sector SPDR,etf,sector
XLU,XLU,macro,Utilities Select Sector SPDR,etf,sector
```

- [ ] **Step 2: Register seed column types in dbt_project.yml**

In `warehouse/dbt_project.yml`, add under `seeds: dadayu_warehouse:`:

```yaml
    macro_universe:
      +column_types:
        macro_id: text
        ticker: text
        market: text
        name: text
        instrument_type: text
        regime_dimension: text
```

- [ ] **Step 3: Add macro_prices_daily table to postgres_init.sql**

Append to `db/postgres_init.sql` after the `index_prices_daily` block (before `RESET ROLE`):

```sql
CREATE TABLE IF NOT EXISTS macro_prices_daily (
    ticker      text NOT NULL,
    market      text NOT NULL DEFAULT 'macro',
    date        date NOT NULL,
    open        double precision,
    high        double precision,
    low         double precision,
    close       double precision,
    volume      bigint NOT NULL DEFAULT 0,
    ingested_at timestamp NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (ticker, market, date)
);
```

- [ ] **Step 4: Apply DDL in running Postgres**

```bash
docker compose exec dadayu_postgres psql -U dadayu -d dadayu \
  -c "CREATE TABLE IF NOT EXISTS dadayu.macro_prices_daily (
        ticker text NOT NULL,
        market text NOT NULL DEFAULT 'macro',
        date date NOT NULL,
        open double precision,
        high double precision,
        low double precision,
        close double precision,
        volume bigint NOT NULL DEFAULT 0,
        ingested_at timestamp NOT NULL DEFAULT current_timestamp,
        PRIMARY KEY (ticker, market, date)
      );"
```

Expected: `CREATE TABLE`

- [ ] **Step 5: Commit**

```bash
git add warehouse/seeds/macro_universe.csv warehouse/dbt_project.yml db/postgres_init.sql
git commit -m "feat(macro): add macro_universe seed and macro_prices_daily Postgres table"
```

---

## Task 2: Ingest Module

**Files:**
- Create: `dadayu/ingest/macro.py`
- Modify: `tests/test_dagster_assets.py` (add load_symbols test)

- [ ] **Step 1: Write failing test**

Add to `tests/test_dagster_assets.py`:

```python
from dadayu.ingest.macro import load_symbols, load_universe


def test_macro_load_symbols_returns_26_tickers():
    symbols = load_symbols()
    assert len(symbols) == 26
    assert "HYG" in symbols
    assert "^TNX" in symbols
    assert "CL=F" in symbols
    assert "HG=F" in symbols


def test_macro_load_universe_has_required_columns():
    universe = load_universe()
    assert len(universe) == 26
    row = next(r for r in universe if r["ticker"] == "HYG")
    assert row["market"] == "macro"
    assert row["instrument_type"] == "etf"
    assert row["regime_dimension"] == "credit"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_dagster_assets.py::test_macro_load_symbols_returns_26_tickers -v
```

Expected: `ModuleNotFoundError: No module named 'dadayu.ingest.macro'`

- [ ] **Step 3: Create dadayu/ingest/macro.py**

```python
from __future__ import annotations

import csv
from pathlib import Path

UNIVERSE_CSV = Path(__file__).parent.parent.parent / "warehouse" / "seeds" / "macro_universe.csv"
INTERVAL_TABLE = {"1d": "macro_prices_daily"}
MARKET = "macro"


def load_universe() -> list[dict]:
    with open(UNIVERSE_CSV, newline="") as f:
        return list(csv.DictReader(f))


def load_symbols() -> list[str]:
    return [row["ticker"] for row in load_universe()]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_dagster_assets.py::test_macro_load_symbols_returns_26_tickers tests/test_dagster_assets.py::test_macro_load_universe_has_required_columns -v
```

Expected: 2 PASSED

- [ ] **Step 5: Commit**

```bash
git add dadayu/ingest/macro.py tests/test_dagster_assets.py
git commit -m "feat(macro): add ingest module with load_symbols and load_universe"
```

---

## Task 3: Dagster Asset + Wiring + Test

**Files:**
- Create: `dagster_pipeline/assets/macro.py`
- Modify: `dagster_pipeline/assets/__init__.py`
- Modify: `dagster_pipeline/schedules.py`
- Modify: `dagster_pipeline/definitions.py`
- Modify: `tests/test_dagster_assets.py`

- [ ] **Step 1: Write failing test**

Add to `tests/test_dagster_assets.py`:

```python
from unittest.mock import MagicMock, patch

import pandas as pd
from dagster import materialize

from dagster_pipeline.assets.macro import macro_ohlcv
from dagster_pipeline.resources import PostgresResource


def test_macro_ohlcv_skips_empty_download():
    with patch("dagster_pipeline.assets.macro.load_symbols", return_value=["HYG"]), \
         patch("dagster_pipeline.assets.macro.get_watermark", return_value="2026-05-01"), \
         patch("dagster_pipeline.assets.macro.download_ohlcv", return_value=pd.DataFrame()):
        result = materialize(
            [macro_ohlcv],
            resources={"postgres": PostgresResource()},
        )
    assert result.success
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_dagster_assets.py::test_macro_ohlcv_skips_empty_download -v
```

Expected: `ModuleNotFoundError: No module named 'dagster_pipeline.assets.macro'`

- [ ] **Step 3: Create dagster_pipeline/assets/macro.py**

```python
from __future__ import annotations

import pandas as pd
from dagster import asset

from dadayu.ingest.equity import download_ohlcv
from dadayu.ingest.macro import INTERVAL_TABLE, MARKET, load_symbols
from dadayu.insert import insert_ohlcv
from dadayu.watermark import get_watermark
from dagster_pipeline.resources import PostgresResource


def _five_year_backfill_start() -> str:
    return (pd.Timestamp.today() - pd.DateOffset(years=5)).strftime("%Y-%m-%d")


@asset(group_name="ingestion")
def macro_ohlcv(postgres: PostgresResource) -> None:
    client = postgres.get_client()
    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    symbols = load_symbols()
    for interval, table in INTERVAL_TABLE.items():
        start = get_watermark(client, table, "date", market=MARKET) or _five_year_backfill_start()
        if pd.Timestamp(start) > pd.Timestamp(today):
            print(f"  {table} [{MARKET}] is already current through {today}")
            continue
        prices = download_ohlcv(symbols, start, today, interval)
        if prices.empty:
            continue
        insert_ohlcv(client, table, prices, MARKET, interval)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pytest tests/test_dagster_assets.py::test_macro_ohlcv_skips_empty_download -v
```

Expected: PASSED

- [ ] **Step 5: Export from assets __init__.py**

The file `dagster_pipeline/assets/__init__.py` currently has `__all__: list[str] = []`. Keep the docstring, add:

```python
"""Dagster asset modules.

Keep this package initializer lightweight so importing one asset module does not
eagerly construct every dbt-backed asset definition.
"""

__all__: list[str] = []
```

(No change needed — the `__init__.py` is intentionally thin; imports happen in `definitions.py` and `schedules.py` directly.)

- [ ] **Step 6: Add macro_job and macro_schedule to schedules.py**

In `dagster_pipeline/schedules.py`, add the import alongside existing imports:

```python
from dagster_pipeline.assets.macro import macro_ohlcv
```

Then add these two definitions after the `index_schedule` block:

```python
macro_job = define_asset_job(
    name="macro_job",
    selection=AssetSelection.assets(macro_ohlcv, *_DBT_DAILY_ASSETS),
)

# Daily at 22:15 UTC on weekdays (15 min after equity, before index at 22:30).
macro_schedule = ScheduleDefinition(
    job=macro_job,
    cron_schedule="15 22 * * 1-5",
)
```

- [ ] **Step 7: Register asset and schedule in definitions.py**

Full updated `dagster_pipeline/definitions.py` (Postgres-native, no polymarket):

```python
from __future__ import annotations

from dagster import Definitions
from dagster_dbt import DbtCliResource

from dagster_pipeline.assets.backtests import backtest_performance_log
from dagster_pipeline.assets.crypto import crypto_info, crypto_ohlcv
from dagster_pipeline.assets.dbt import (
    data_quality,
    dbt_backtest_assets,
    dbt_mart_assets,
    dbt_seed_assets,
    dbt_snapshot_assets,
    dbt_staging_assets,
)
from dagster_pipeline.assets.dbt._common import DBT_PROJECT_DIR
from dagster_pipeline.assets.equity import equity_ohlcv, equity_ticker_info
from dagster_pipeline.assets.indexes import index_ohlcv
from dagster_pipeline.assets.macro import macro_ohlcv
from dagster_pipeline.assets.product import portfolio_ranker_top_20_log
from dagster_pipeline.resources import PostgresResource
from dagster_pipeline.schedules import (
    backtest_job,
    backtest_schedule,
    crypto_job,
    crypto_schedule,
    equity_job,
    equity_schedule,
    index_job,
    index_schedule,
    macro_job,
    macro_schedule,
    portfolio_ranker_job,
    portfolio_ranker_schedule,
    snapshot_job,
)

defs = Definitions(
    assets=[
        equity_ohlcv,
        equity_ticker_info,
        index_ohlcv,
        macro_ohlcv,
        crypto_ohlcv,
        crypto_info,
        dbt_seed_assets,
        dbt_staging_assets,
        dbt_snapshot_assets,
        dbt_mart_assets,
        dbt_backtest_assets,
        data_quality,
        portfolio_ranker_top_20_log,
        backtest_performance_log,
    ],
    resources={
        "postgres": PostgresResource(),
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
    jobs=[
        equity_job,
        index_job,
        macro_job,
        crypto_job,
        portfolio_ranker_job,
        snapshot_job,
        backtest_job,
    ],
    schedules=[
        equity_schedule,
        index_schedule,
        macro_schedule,
        crypto_schedule,
        portfolio_ranker_schedule,
        backtest_schedule,
    ],
)
```

- [ ] **Step 8: Run full test suite to confirm no regressions**

```bash
pytest tests/ -v
```

Expected: all tests PASSED (including the new macro test)

- [ ] **Step 9: Commit**

```bash
git add dagster_pipeline/assets/macro.py \
        dagster_pipeline/schedules.py dagster_pipeline/definitions.py \
        tests/test_dagster_assets.py
git commit -m "feat(macro): add macro_ohlcv Dagster asset and 22:15 UTC weekday schedule"
```

---

## Task 4: dbt Source + Staging

**Files:**
- Modify: `warehouse/models/01_staging/yahoo/_sources.yml`
- Create: `warehouse/models/01_staging/yahoo/stg_yahoo__macro_ohlcv_daily.sql`

- [ ] **Step 1: Add macro_prices_daily source to _sources.yml**

In `warehouse/models/01_staging/yahoo/_sources.yml`, append under the `tables:` list:

```yaml
      - name: macro_prices_daily
        description: Daily macro cross-asset OHLCV bars (ETFs, rate indices, futures) ingested from yfinance.
        freshness:
          warn_after: {count: 2, period: day}
          error_after: {count: 5, period: day}
        loaded_at_field: ingested_at
```

- [ ] **Step 2: Create stg_yahoo__macro_ohlcv_daily.sql**

```sql
{{ config(
    materialized='incremental',
    unique_key=['ticker', 'market', 'ts'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    indexes=[
        {'columns': ['ticker', 'market', 'ts'], 'unique': True},
    ],
) }}

WITH source AS (
    SELECT * FROM {{ source('yahoo', 'macro_prices_daily') }} AS source_raw
    {% if is_incremental() %}
    WHERE date > (SELECT COALESCE(MAX(ts)::date, DATE '1900-01-01') FROM {{ this }})
       OR NOT EXISTS (
            SELECT 1
            FROM {{ this }} AS target
            WHERE target.ticker = source_raw.ticker
              AND target.market = source_raw.market
              AND target.ts = source_raw.date::timestamp
       )
    {% endif %}
),

universe AS (
    SELECT ticker, name, instrument_type, regime_dimension
    FROM {{ ref('macro_universe') }}
)

SELECT
    s.ticker,
    s.market,
    s.date::timestamp AS ts,
    s.open,
    s.high,
    s.low,
    s.close,
    COALESCE(s.volume, 0) AS volume,
    u.name,
    u.instrument_type,
    u.regime_dimension,
    s.ingested_at
FROM source AS s
LEFT JOIN universe AS u ON s.ticker = u.ticker
WHERE s.close > 0
```

- [ ] **Step 3: Run dbt parse to verify no syntax errors**

```bash
docker compose run --rm dadayu_dbt parse
```

Expected: `Done. PASS=... WARN=0 ERROR=0`

- [ ] **Step 4: Commit**

```bash
git add warehouse/models/01_staging/yahoo/_sources.yml \
        warehouse/models/01_staging/yahoo/stg_yahoo__macro_ohlcv_daily.sql
git commit -m "feat(macro): add macro_prices_daily dbt source and staging model"
```

---

## Task 5: Intermediate — Per-Ticker Indicators

**Files:**
- Create: `warehouse/models/02_intermediate/macro/int_macro_assets_daily.sql`

- [ ] **Step 1: Create the directory and model**

```sql
{{ config(
    materialized='table',
) }}

WITH base AS (
    SELECT
        ticker,
        ts,
        close,
        instrument_type,
        regime_dimension,
        LAG(close, 1)  OVER (PARTITION BY ticker ORDER BY ts) AS prev_close_1d,
        LAG(close, 20) OVER (PARTITION BY ticker ORDER BY ts) AS prev_close_20d,
        LAG(close, 60) OVER (PARTITION BY ticker ORDER BY ts) AS prev_close_60d,
        AVG(close) OVER (PARTITION BY ticker ORDER BY ts ROWS BETWEEN 19  PRECEDING AND CURRENT ROW) AS sma_20,
        AVG(close) OVER (PARTITION BY ticker ORDER BY ts ROWS BETWEEN 49  PRECEDING AND CURRENT ROW) AS sma_50,
        AVG(close) OVER (PARTITION BY ticker ORDER BY ts ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma_200
    FROM {{ ref('stg_yahoo__macro_ohlcv_daily') }}
    WHERE close > 0
)

SELECT
    ticker,
    ts,
    close,
    instrument_type,
    regime_dimension,
    CASE WHEN prev_close_1d  IS NOT NULL AND prev_close_1d  <> 0
         THEN (close / prev_close_1d  - 1) * 100 END AS return_pct,
    CASE WHEN prev_close_20d IS NOT NULL AND prev_close_20d <> 0
         THEN (close / prev_close_20d - 1) * 100 END AS return_20d_pct,
    CASE WHEN prev_close_60d IS NOT NULL AND prev_close_60d <> 0
         THEN (close / prev_close_60d - 1) * 100 END AS return_60d_pct,
    sma_20,
    sma_50,
    sma_200,
    CASE WHEN close > sma_20  THEN 1 ELSE 0 END AS above_sma_20,
    CASE WHEN close > sma_50  THEN 1 ELSE 0 END AS above_sma_50,
    CASE WHEN close > sma_200 THEN 1 ELSE 0 END AS above_sma_200,
    0.0 AS pct_rank_return_20d
FROM base
```

- [ ] **Step 2: Run dbt parse**

```bash
docker compose run --rm dadayu_dbt parse
```

Expected: `Done. PASS=... WARN=0 ERROR=0`

- [ ] **Step 3: Commit**

```bash
git add warehouse/models/02_intermediate/macro/int_macro_assets_daily.sql
git commit -m "feat(macro): add int_macro_assets_daily with rolling returns and SMAs"
```

---

## Task 6: Intermediate — Composite Regime + Passthrough

**Files:**
- Create: `warehouse/models/02_intermediate/market/int_macro_regime_daily.sql`
- Modify: `warehouse/models/02_intermediate/market/int_market_regime_daily.sql`
- Modify: `warehouse/models/02_intermediate/schema.yml`

- [ ] **Step 1: Create int_macro_regime_daily.sql**

```sql
{{ config(
    materialized='table',
) }}

-- Pivot: one row per date, one column per ticker indicator.
-- FILTER aggregation selects signal for a specific ticker on a given date.
-- COALESCE to 0 handles rare data gaps for liquid instruments.
WITH pivoted AS (
    SELECT
        ts,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = 'HYG'),  0) AS hyg_return_20d,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'HYG'),  0) AS hyg_above_sma50,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = 'LQD'),  0) AS lqd_return_20d,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'LQD'),  0) AS lqd_above_sma50,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = '^TNX'), 0) AS tnx_return_20d,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = 'TLT'),  0) AS tlt_return_20d,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'VNQ'),  0) AS vnq_above_sma50,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = 'GLD'),  0) AS gld_return_20d,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = 'CL=F'), 0) AS clf_return_20d,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'TIP'),  0) AS tip_above_sma50,
        COALESCE(MAX(above_sma_20)    FILTER (WHERE ticker = 'UUP'),  0) AS uup_above_sma20,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'UUP'),  0) AS uup_above_sma50,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = 'UUP'),  0) AS uup_return_20d,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'DBB'),  0) AS dbb_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'CPER'), 0) AS cper_above_sma50,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = 'HG=F'), 0) AS hgf_return_20d,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'EEM'),  0) AS eem_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'EFA'),  0) AS efa_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'XLK'),  0) AS xlk_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'XLF'),  0) AS xlf_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'XLI'),  0) AS xli_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'XLE'),  0) AS xle_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'XLU'),  0) AS xlu_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'XLV'),  0) AS xlv_above_sma50
    FROM {{ ref('int_macro_assets_daily') }}
    GROUP BY ts
),

-- credit_score computed first because rates_score depends on it.
with_credit AS (
    SELECT
        *,
        LEAST(100, GREATEST(0,
            CASE WHEN hyg_return_20d > 0  THEN 40 ELSE 0 END +
            CASE WHEN hyg_above_sma50 = 1 THEN 10 ELSE 0 END +
            CASE WHEN lqd_return_20d > 0  THEN 40 ELSE 0 END +
            CASE WHEN lqd_above_sma50 = 1 THEN 10 ELSE 0 END
        ))::double precision AS credit_score
    FROM pivoted
),

with_all_scores AS (
    SELECT
        ts,
        credit_score,
        -- rates_score: uses credit_score to disambiguate TLT rally (easing vs flight-to-safety)
        LEAST(100, GREATEST(0,
            40 +
            CASE WHEN tnx_return_20d < 0                             THEN  30 ELSE 0 END +
            CASE WHEN tnx_return_20d > 1.0                           THEN -20 ELSE 0 END +
            CASE WHEN tlt_return_20d > 2.0 AND credit_score >= 60    THEN  20 ELSE 0 END +
            CASE WHEN tlt_return_20d > 2.0 AND credit_score < 40     THEN -30 ELSE 0 END +
            CASE WHEN vnq_above_sma50 = 1                            THEN  10 ELSE 0 END
        ))::double precision AS rates_score,
        -- inflation_score: commodity spike = equity drag; disinflation = equity-friendly
        LEAST(100, GREATEST(0,
            60 +
            CASE WHEN gld_return_20d > 3.0 AND clf_return_20d > 3.0  THEN -40 ELSE 0 END +
            CASE WHEN tip_above_sma50 = 1                             THEN -20 ELSE 0 END +
            CASE WHEN gld_return_20d < 0   AND clf_return_20d < 0     THEN  30 ELSE 0 END
        ))::double precision AS inflation_score,
        -- dollar_score: strong dollar = risk-off for equities/commodities/EM
        LEAST(100, GREATEST(0,
            80 +
            CASE WHEN uup_above_sma20 = 1    THEN -40 ELSE 0 END +
            CASE WHEN uup_above_sma50 = 1    THEN -20 ELSE 0 END +
            CASE WHEN uup_return_20d > 2.0   THEN -20 ELSE 0 END
        ))::double precision AS dollar_score,
        -- growth_score: industrial metals + EM = global growth proxy
        LEAST(100, GREATEST(0,
            10 +
            CASE WHEN dbb_above_sma50  = 1  THEN 20 ELSE 0 END +
            CASE WHEN cper_above_sma50 = 1  THEN 20 ELSE 0 END +
            CASE WHEN hgf_return_20d   > 0  THEN 15 ELSE 0 END +
            CASE WHEN eem_above_sma50  = 1  THEN 20 ELSE 0 END +
            CASE WHEN efa_above_sma50  = 1  THEN 15 ELSE 0 END
        ))::double precision AS growth_score,
        -- sector_score: cyclicals leading = risk-on; defensives leading = risk-off
        LEAST(100, GREATEST(0,
            30 +
            CASE WHEN xlk_above_sma50 = 1   THEN  15 ELSE 0 END +
            CASE WHEN xlf_above_sma50 = 1   THEN  15 ELSE 0 END +
            CASE WHEN xli_above_sma50 = 1   THEN  15 ELSE 0 END +
            CASE WHEN xle_above_sma50 = 1   THEN  10 ELSE 0 END +
            CASE WHEN xlu_above_sma50 = 1   THEN -20 ELSE 0 END +
            CASE WHEN xlv_above_sma50 = 1   THEN -15 ELSE 0 END
        ))::double precision AS sector_score
    FROM with_credit
),

with_composite AS (
    SELECT
        ts,
        credit_score,
        rates_score,
        inflation_score,
        dollar_score,
        growth_score,
        sector_score,
        ROUND((
            credit_score    * 0.25 +
            growth_score    * 0.25 +
            dollar_score    * 0.15 +
            sector_score    * 0.15 +
            rates_score     * 0.10 +
            inflation_score * 0.10
        )::numeric, 2)::double precision AS composite_macro_score
    FROM with_all_scores
),

benchmark AS (
    SELECT
        ts,
        close           AS benchmark_close,
        return_pct      AS benchmark_return_pct,
        CASE WHEN close > sma_200 THEN TRUE ELSE FALSE END AS benchmark_above_sma_200
    FROM {{ ref('int_market_indexes_daily') }}
    WHERE index_id = 'sp500'
)

SELECT
    c.ts,
    c.credit_score,
    c.rates_score,
    c.inflation_score,
    c.dollar_score,
    c.growth_score,
    c.sector_score,
    c.composite_macro_score,
    CASE
        WHEN c.composite_macro_score >= 80 THEN 'risk_on'
        WHEN c.composite_macro_score >= 60 THEN 'constructive'
        WHEN c.composite_macro_score >= 40 THEN 'neutral'
        WHEN c.composite_macro_score >= 20 THEN 'defensive'
        ELSE 'risk_off'
    END AS macro_regime,
    -- Legacy backward-compat: downstream models read market_regime (3-state) and risk_on_score
    CASE
        WHEN c.composite_macro_score >= 60 THEN 'risk_on'
        WHEN c.composite_macro_score >= 40 THEN 'neutral'
        ELSE 'risk_off'
    END AS market_regime,
    c.composite_macro_score AS risk_on_score,
    b.benchmark_close,
    b.benchmark_return_pct,
    b.benchmark_above_sma_200
FROM with_composite AS c
LEFT JOIN benchmark AS b ON c.ts = b.ts
```

- [ ] **Step 2: Update int_market_regime_daily.sql to passthrough**

Replace the entire contents of `warehouse/models/02_intermediate/market/int_market_regime_daily.sql` with:

```sql
{{ config(
    materialized='table',
) }}

SELECT * FROM {{ ref('int_macro_regime_daily') }}
```

- [ ] **Step 3: Add entries to warehouse/models/02_intermediate/schema.yml**

Append to the end of the `models:` list in `warehouse/models/02_intermediate/schema.yml`:

```yaml
  - name: int_macro_assets_daily
    description: Daily per-ticker indicators for the macro cross-asset universe (26 instruments).
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [ticker, ts]
    columns:
      - name: ticker
        data_tests: [not_null]
      - name: ts
        data_tests: [not_null]

  - name: int_macro_regime_daily
    description: Daily 6-dimension composite macro regime score (0-100) and 5-state label.
    columns:
      - name: ts
        data_tests: [not_null]
      - name: composite_macro_score
        data_tests: [not_null]
      - name: macro_regime
        data_tests:
          - not_null
          - accepted_values:
              values: [risk_on, constructive, neutral, defensive, risk_off]
      - name: market_regime
        data_tests:
          - accepted_values:
              values: [risk_on, neutral, risk_off]
```

- [ ] **Step 4: Run dbt parse**

```bash
docker compose run --rm dadayu_dbt parse
```

Expected: `Done. PASS=... WARN=0 ERROR=0`

- [ ] **Step 5: Commit**

```bash
git add warehouse/models/02_intermediate/market/int_macro_regime_daily.sql \
        warehouse/models/02_intermediate/market/int_market_regime_daily.sql \
        warehouse/models/02_intermediate/schema.yml
git commit -m "feat(macro): add int_macro_regime_daily with 6-dimension composite score; int_market_regime_daily → passthrough"
```

---

## Task 7: Mart + Schema

**Files:**
- Create: `warehouse/models/03_marts/macro/mart_macro_regime_daily.sql`
- Create: `warehouse/models/03_marts/macro/schema.yml`

- [ ] **Step 1: Create mart_macro_regime_daily.sql**

```sql
{{ config(
    materialized='table',
) }}

SELECT
    ts,
    composite_macro_score,
    AVG(composite_macro_score)
        OVER (ORDER BY ts ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
        AS composite_macro_score_30d_avg,
    composite_macro_score
        - AVG(composite_macro_score)
            OVER (ORDER BY ts ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
        AS composite_macro_score_trend,
    credit_score,
    rates_score,
    inflation_score,
    dollar_score,
    growth_score,
    sector_score,
    macro_regime,
    market_regime,
    risk_on_score,
    benchmark_close,
    benchmark_return_pct,
    benchmark_above_sma_200,
    -- Hysteresis constants stored here so downstream consumers don't hardcode thresholds
    70.0 AS risk_on_entry_threshold,
    55.0 AS risk_on_exit_threshold,
    30.0 AS risk_off_entry_threshold,
    45.0 AS risk_off_exit_threshold
FROM {{ ref('int_macro_regime_daily') }}
ORDER BY ts
```

- [ ] **Step 2: Create warehouse/models/03_marts/macro/schema.yml**

```yaml
version: 2

models:
  - name: mart_macro_regime_daily
    description: >
      Metabase-facing macro regime mart. One row per trading day.
      composite_macro_score (0–100) is the weighted composite of 6 dimension scores.
      macro_regime is the 5-state label (risk_on / constructive / neutral / defensive / risk_off).
      Hysteresis entry/exit thresholds stored as constant columns for strategy consumers.
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [ts]
    columns:
      - name: ts
        data_tests: [not_null]
      - name: composite_macro_score
        data_tests: [not_null]
      - name: credit_score
        data_tests: [not_null]
      - name: rates_score
        data_tests: [not_null]
      - name: inflation_score
        data_tests: [not_null]
      - name: dollar_score
        data_tests: [not_null]
      - name: growth_score
        data_tests: [not_null]
      - name: sector_score
        data_tests: [not_null]
      - name: macro_regime
        data_tests:
          - not_null
          - accepted_values:
              values: [risk_on, constructive, neutral, defensive, risk_off]
```

- [ ] **Step 3: Run dbt parse**

```bash
docker compose run --rm dadayu_dbt parse
```

Expected: `Done. PASS=... WARN=0 ERROR=0`

- [ ] **Step 4: Commit**

```bash
git add warehouse/models/03_marts/macro/mart_macro_regime_daily.sql \
        warehouse/models/03_marts/macro/schema.yml
git commit -m "feat(macro): add mart_macro_regime_daily with 30d smoothing and hysteresis constants"
```

---

## Task 8: End-to-End Smoke Test

**Files:** none (verification only)

- [ ] **Step 1: Start Postgres and run baseline tests**

```bash
docker compose up -d dadayu_postgres
pytest tests/ -x -q
```

Expected: 29+ tests PASSED, no failures.

- [ ] **Step 2: Seed macro_universe**

```bash
docker compose run --rm dadayu_dbt seed --select macro_universe
```

Expected: `Completed successfully. PASS=1`

- [ ] **Step 3: Run macro_ohlcv backfill (5 years)**

Via Dagster materialize CLI or Dagster UI:

```bash
cd dagster_pipeline && dagster asset materialize -m dagster_pipeline.definitions --select macro_ohlcv
```

Or, if Docker Dagster is running, navigate to Assets → macro_ohlcv → Materialize.

Expected: completes without error; `macro_prices_daily` table has rows.

Verify row count:
```bash
docker compose exec dadayu_postgres psql -U dadayu -d dadayu \
  -c "SELECT count(*), min(date), max(date) FROM dadayu.macro_prices_daily;"
```

Expected: ~30,000–35,000 rows (26 tickers × ~5 years of trading days), date range 2021 to present.

- [ ] **Step 4: Run dbt models for macro pipeline**

```bash
docker compose run --rm dadayu_dbt run \
  --select stg_yahoo__macro_ohlcv_daily int_macro_assets_daily int_macro_regime_daily mart_macro_regime_daily
```

Expected: `Completed successfully. PASS=4 WARN=0 ERROR=0`

- [ ] **Step 5: Run dbt tests**

```bash
docker compose run --rm dadayu_dbt test \
  --select stg_yahoo__macro_ohlcv_daily int_macro_assets_daily int_macro_regime_daily mart_macro_regime_daily
```

Expected: all tests PASS.

- [ ] **Step 6: Verify passthrough — downstream models still run**

```bash
docker compose run --rm dadayu_dbt run --select int_market_regime_daily+
```

Expected: `Completed successfully.` (no downstream failures from the passthrough change)

- [ ] **Step 7: Sample output spot-check**

```bash
docker compose exec dadayu_postgres psql -U dadayu -d dadayu -c \
  "SELECT ts, composite_macro_score, macro_regime, credit_score, growth_score
   FROM dadayu.mart_macro_regime_daily
   ORDER BY ts DESC
   LIMIT 5;"
```

Expected: 5 rows with composite_macro_score in [0, 100] and macro_regime in the accepted 5 values.

- [ ] **Step 8: Run full pytest suite**

```bash
pytest tests/ -v
```

Expected: all tests PASS, no regressions.

- [ ] **Step 9: Final commit**

```bash
git add .
git commit -m "feat(macro): complete macro regime pipeline — end-to-end verified"
```

---

## Appendix: Score Reference

| Dimension | Base | Min | Max | Key signals |
|-----------|------|-----|-----|-------------|
| credit | 0 | 0 | 100 | HYG/LQD return + SMA50 |
| rates | 40 | 0 | 100 | ^TNX direction, TLT+credit context, VNQ |
| inflation | 60 | 0 | 100 | GLD+CL=F spike/disinflation, TIP |
| dollar | 80 | 0 | 100 | UUP SMA20/50, UUP 20d return |
| growth | 10 | 0 | 100 | DBB/CPER/HG=F, EEM/EFA |
| sector | 30 | 0 | 100 | cyclicals (+) vs defensives (-) |

Composite = credit×0.25 + growth×0.25 + dollar×0.15 + sector×0.15 + rates×0.10 + inflation×0.10

| Score | 5-state label | Legacy 3-state |
|-------|---------------|----------------|
| ≥ 80 | risk_on | risk_on |
| ≥ 60 | constructive | risk_on |
| ≥ 40 | neutral | neutral |
| ≥ 20 | defensive | risk_off |
| < 20 | risk_off | risk_off |

Hysteresis (for strategy use, not dashboard labels): enter risk_on ≥70, exit <55; enter risk_off ≤30, exit >45.
