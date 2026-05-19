# Dagster Orchestration Design

## Goal

Add Dagster as a local orchestrator for the DADAYU AI data pipeline — scheduling ingestion, running dbt, and surfacing the full asset lineage in a web UI.

## Architecture

Two layers of assets in a single Dagster package (`dagster_pipeline/`) that runs locally via `dagster dev`.

**Ingestion assets** call into the existing `dadayu/` package — no business logic duplication. The 4 CLI scripts stay runnable manually.

**dbt assets** are auto-generated from `warehouse/manifest.json` via `@dbt_assets`. A custom `DagsterDbtTranslator` maps dbt source tables to upstream ingestion asset keys, giving Dagster the full end-to-end lineage graph.

## Tech Stack

- `dagster` + `dagster-webserver` — local UI at `localhost:3000`
- `dagster-dbt` — dbt integration
- `dagster_home/` — local sqlite run history (gitignored)

## File Structure

```
dagster_pipeline/
  __init__.py
  assets/
    __init__.py
    equity.py        — equity_ohlcv, equity_ticker_info assets
    crypto.py        — crypto_ohlcv, crypto_info assets
    dbt_assets.py    — @dbt_assets wrapping warehouse/
  resources.py       — ClickhouseResource (ConfigurableResource wrapping get_ch_client)
                       DbtCliResource pointing at warehouse/
  schedules.py       — equity_job, crypto_job, cron schedules
  definitions.py     — Dagster Definitions entry point

dagster_home/        — local run metadata (gitignored)
```

## Assets

### Ingestion Assets (group: `ingestion`)

| Asset | Upstream deps | What it does |
|---|---|---|
| `equity_ohlcv` | none | Downloads equity OHLCV for germany/us/india, all intervals (1h/4h/1d), watermark-based |
| `equity_ticker_info` | `equity_ohlcv` | Fetches yfinance metadata for all tickers loaded in prices_daily |
| `crypto_ohlcv` | none | Downloads crypto OHLCV for all intervals, watermark-based |
| `crypto_info` | `crypto_ohlcv` | Fetches CoinGecko metadata for top-20 coins |

### dbt Assets (group: `dbt`)

Auto-generated — one asset per dbt model (32 total). The `DagsterDbtTranslator` maps:

- dbt sources `prices_hourly / prices_4h / prices_daily` → `equity_ohlcv`
- dbt sources `crypto_prices_*` → `crypto_ohlcv`
- dbt source `tickers` → `equity_ticker_info`
- dbt source `crypto_metadata` → `crypto_info`

Full lineage in Dagster UI:
```
equity_ohlcv → stg_yahoo__ohlcv_*     → int_equity_ohlcv_* → fct_ohlcv_*         → fct_indicators_*
crypto_ohlcv → stg_yahoo__crypto_ohlcv_* → int_crypto_ohlcv_* → fct_ohlcv_crypto_* → fct_indicators_crypto_*
equity_ticker_info → snap_dim_equity_symbol → dim_equity_symbol
crypto_info  → stg_coingecko__* → snap_dim_crypto_symbol → dim_crypto_symbol
```

## Schedules

### `equity_job`
Selects: `equity_ohlcv`, `equity_ticker_info`, all dbt equity models.
Steps: ingestion → `dbt run --select <equity models>` → `dbt test --select <equity models>`.
Schedule: `"0 22 * * 1-5"` — daily at 22:00 UTC on weekdays (after all equity markets close).

### `crypto_job`
Selects: `crypto_ohlcv`, `crypto_info`, all dbt crypto models.
Steps: ingestion → `dbt run --select <crypto models>` → `dbt test --select <crypto models>`.
Schedule: `"0 */4 * * *"` — every 4 hours, 24/7.

Both jobs are also manually triggerable from the Dagster UI.

## Resources

**`ClickhouseResource`** — `ConfigurableResource` wrapping `dadayu.db.get_ch_client()`. Injected into all 4 ingestion assets. Reads host/port/db/user/password from environment (same `.env` as today).

**`DbtCliResource`** — points at `warehouse/` directory. Used by `@dbt_assets` to run and test dbt models.

## Error Handling

- Ingestion asset fails → Dagster marks failed, downstream dbt does not run
- dbt test fails → asset marked failed, full dbt output in Dagster logs
- Watermark unchanged on failure — next run retries from same point automatically
- No auto-retry (local dev; re-trigger manually from UI)

## Running Locally

```bash
# install
pip install dagster dagster-webserver dagster-dbt

# generate dbt manifest (required for @dbt_assets)
cd warehouse && dbt parse && cd ..

# start Dagster UI
DAGSTER_HOME=./dagster_home dagster dev -m dagster_pipeline
# → open http://localhost:3000
```

## Testing

Asset unit tests use `dagster.materialize()` with a mock `ClickhouseResource` — no real ClickHouse needed. Existing `tests/` pytest suite unchanged.
