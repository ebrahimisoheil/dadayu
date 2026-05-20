from __future__ import annotations

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

from dagster_pipeline.assets.dbt_assets import dadayu_dbt_assets
from dagster_pipeline.assets.equity import equity_ohlcv, equity_ticker_info
from dagster_pipeline.assets.crypto import crypto_ohlcv, crypto_info

# Equity job: ingest equity then run all dbt models
equity_job = define_asset_job(
    name="equity_job",
    selection=AssetSelection.assets(
        equity_ohlcv, equity_ticker_info, dadayu_dbt_assets
    ),
)

# Crypto job: ingest crypto then run all dbt models
crypto_job = define_asset_job(
    name="crypto_job",
    selection=AssetSelection.assets(
        crypto_ohlcv, crypto_info, dadayu_dbt_assets
    ),
)

# Daily at 22:00 UTC on weekdays (after all equity markets close)
equity_schedule = ScheduleDefinition(
    job=equity_job,
    cron_schedule="0 22 * * 1-5",
)

# Every 4 hours, 24/7 (crypto never closes)
crypto_schedule = ScheduleDefinition(
    job=crypto_job,
    cron_schedule="0 */4 * * *",
)
