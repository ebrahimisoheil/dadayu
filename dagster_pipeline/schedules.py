from __future__ import annotations

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

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
from dagster_pipeline.assets.equity import equity_ohlcv, equity_ticker_info
from dagster_pipeline.assets.indexes import index_ohlcv
from dagster_pipeline.assets.macro import macro_ohlcv
from dagster_pipeline.assets.product import portfolio_ranker_top_20_log

_DBT_DAILY_ASSETS = [dbt_seed_assets, dbt_staging_assets, dbt_mart_assets]
_DBT_SNAPSHOT_ASSETS = [dbt_seed_assets, dbt_staging_assets, dbt_snapshot_assets]

equity_job = define_asset_job(
    name="equity_job",
    selection=AssetSelection.assets(equity_ohlcv, equity_ticker_info, *_DBT_DAILY_ASSETS),
)

crypto_job = define_asset_job(
    name="crypto_job",
    selection=AssetSelection.assets(crypto_ohlcv, crypto_info, *_DBT_DAILY_ASSETS),
)

index_job = define_asset_job(
    name="index_job",
    selection=AssetSelection.assets(index_ohlcv, *_DBT_DAILY_ASSETS),
)

macro_job = define_asset_job(
    name="macro_job",
    selection=AssetSelection.assets(macro_ohlcv, *_DBT_DAILY_ASSETS),
)

portfolio_ranker_job = define_asset_job(
    name="portfolio_ranker_job",
    selection=(
        AssetSelection.assets(*_DBT_DAILY_ASSETS)
        | AssetSelection.assets(portfolio_ranker_top_20_log)
    ),
)

snapshot_job = define_asset_job(
    name="snapshot_job",
    selection=AssetSelection.assets(*_DBT_SNAPSHOT_ASSETS),
)

backtest_job = define_asset_job(
    name="backtest_job",
    selection=(
        AssetSelection.assets(dbt_backtest_assets, data_quality)
        | AssetSelection.assets(backtest_performance_log)
    ),
)

# Daily at 22:00 UTC on weekdays (after all equity markets close)
equity_schedule = ScheduleDefinition(
    job=equity_job,
    cron_schedule="0 22 * * 1-5",
)

# Daily at 01:00 UTC. Crypto prices are stored daily only.
crypto_schedule = ScheduleDefinition(
    job=crypto_job,
    cron_schedule="0 1 * * *",
)

# Daily at 22:30 UTC after the equity batch starts.
index_schedule = ScheduleDefinition(
    job=index_job,
    cron_schedule="30 22 * * 1-5",
)

# Daily at 22:15 UTC on weekdays (15 min after equity, before index at 22:30).
macro_schedule = ScheduleDefinition(
    job=macro_job,
    cron_schedule="15 22 * * 1-5",
)

# Daily at 23:00 UTC on weekdays, after market/index refresh.
portfolio_ranker_schedule = ScheduleDefinition(
    job=portfolio_ranker_job,
    cron_schedule="0 23 * * 1-5",
)

# Weekly Sunday 02:00 UTC; backtests are intentionally expensive.
backtest_schedule = ScheduleDefinition(
    job=backtest_job,
    cron_schedule="0 2 * * 0",
)
