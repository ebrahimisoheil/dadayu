# Dagster dbt Modular Asset Design

**Date:** 2026-05-21  
**Status:** Approved  

## Problem

The current `dagster_pipeline/assets/dbt_assets.py` is a single `@dbt_assets` function that tries to sequence `seed → staging → snapshot → marts` internally. This fails because dagster-dbt appends `--select fqn:*` to every `dbt.cli()` call that receives `context=`, which unions with any `--select staging` arg and causes all 65 models to run at once — before seeds or snapshots exist. The result is consistent first-run failures.

## Solution

Split into 5 separate `@dbt_assets` groups. Each group covers a subset of the dbt manifest via `select=`. dagster-dbt reads the manifest and auto-wires dependencies between groups. At runtime, each group's context only injects the fqns for its own models — no `fqn:*` bleed.

## File Structure

```
dagster_pipeline/assets/
├── __init__.py              ← updated exports
├── equity.py                ← unchanged
├── crypto.py                ← unchanged
├── polymarket.py            ← unchanged
└── dbt/
    ├── __init__.py          ← exports all 5 asset groups + data_quality
    ├── _common.py           ← DBT_MANIFEST path + DadayuDbtTranslator (shared)
    ├── seeds.py             ← dbt_seed_assets
    ├── staging.py           ← dbt_staging_assets + dbt test staging
    ├── snapshots.py         ← dbt_snapshot_assets + dbt test snapshots
    ├── marts.py             ← dbt_mart_assets + dbt test marts
    └── quality.py           ← data_quality @asset

dadayu/checks.py             ← extracted check logic (importable)
scripts/check_data_quality.py ← thin CLI wrapper over dadayu.checks (unchanged interface)
```

## Dependency Graph

```
equity_ohlcv ──┐
crypto_ohlcv ──┤
polymarket_* ──┤
               ▼
        dbt_seed_assets          (resource_type:seed)
               │
               ▼
      dbt_staging_assets         (path:staging)
        + dbt test staging
               │
               ▼
      dbt_snapshot_assets        (resource_type:snapshot)
        + dbt test snapshots
               │                        │
               ▼                        ▼
         dbt_mart_assets  ◄─── seeds dep auto-wired via manifest
           + dbt test marts     (int_calendar_sessions → trading_calendar)
               │
               ▼
         data_quality
```

dagster-dbt infers cross-group deps from the manifest automatically. No manual `deps=[]` required between dbt groups.

## Asset Groups Detail

### `_common.py`
- `DBT_PROJECT_DIR`, `DBT_MANIFEST` path constants
- `_SOURCE_UPSTREAM` dict mapping raw ClickHouse source names to Dagster `AssetKey`s
- `DadayuDbtTranslator` class (moved from `dbt_assets.py`, renamed public)

### `seeds.py`
- `@dbt_assets(select="resource_type:seed")`
- Runs `dbt seed`
- No test step (seeds have no dbt tests)

### `staging.py`
- `@dbt_assets(select="path:staging")`
- Runs `dbt run` → `dbt test --select path:staging`
- Upstream deps on ingestion assets wired via `DadayuDbtTranslator`

### `snapshots.py`
- `@dbt_assets(select="resource_type:snapshot")`
- Runs `dbt snapshot` → `dbt test --select resource_type:snapshot`
- Upstream deps on staging auto-wired from manifest

### `marts.py`
- `@dbt_assets(exclude="path:staging resource_type:snapshot resource_type:seed")`
- Runs `dbt run` → `dbt test --exclude path:staging resource_type:snapshot`
- Covers: intermediate models, mart models, reference dims

### `quality.py`
- Regular `@asset(group_name="quality")`
- `deps` on key mart assets: `fct_ohlcv_1h`, `fct_indicators_1d`, `fct_polymarket_signals`, `fct_polymarket_signals`
- Imports and calls functions from `dadayu.checks`
- Raises `Failure` on any FAIL result, logs WARNs as metadata
- Does NOT replace `scripts/check_data_quality.py` — both coexist

## DQ Check Extraction

`scripts/check_data_quality.py` is refactored:

```
dadayu/checks.py
  check_equity_ohlcv(client) → list[CheckResult]
  check_crypto_ohlcv(client) → list[CheckResult]
  check_polymarket(client)   → list[CheckResult]
  check_cross_dataset(client)→ list[CheckResult]
  check_mart_sanity(client)  → list[CheckResult]
  run_all_checks(client)     → list[CheckResult]

scripts/check_data_quality.py
  imports dadayu.checks.run_all_checks
  prints report
  sys.exit(1) on FAIL
```

The Dagster asset calls `run_all_checks`, iterates results, raises `Failure` if any are FAIL, emits metadata for WARNs.

## Schedules

All 3 jobs updated to select all 5 dbt groups + `data_quality`:

```python
equity_job = define_asset_job(
    name="equity_job",
    selection=AssetSelection.assets(
        equity_ohlcv, equity_ticker_info,
        dbt_seed_assets, dbt_staging_assets, dbt_snapshot_assets,
        dbt_mart_assets, data_quality,
    ),
)
# same pattern for crypto_job and polymarket_job
```

## What Does NOT Change

- `dagster_pipeline/resources.py` — unchanged
- `dagster_pipeline/definitions.py` — minor import update only
- `dadayu/watermark.py` — already fixed (epoch-0 guard)
- All dbt SQL models — unchanged
- `db/clickhouse_init.sql` — unchanged
- Schedule cron expressions — unchanged
- Docker setup — unchanged
