from __future__ import annotations

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

from dagster_pipeline.assets.backtests import backtest_performance_log
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
from dagster_pipeline.assets.reports import data_quality_report

_DBT_DAILY_ASSETS = [dbt_seed_assets, dbt_staging_assets, dbt_mart_assets]
_DBT_SNAPSHOT_ASSETS = [dbt_seed_assets, dbt_staging_assets, dbt_snapshot_assets]
_STOCK_INGESTION_ASSETS = [equity_ohlcv, equity_ticker_info, index_ohlcv, macro_ohlcv]
_STOCK_REFRESH_SELECTION = AssetSelection.assets(*_STOCK_INGESTION_ASSETS, *_DBT_DAILY_ASSETS, dbt_snapshot_assets)

equity_job = define_asset_job(
    name="equity_job",
    selection=AssetSelection.assets(equity_ohlcv, equity_ticker_info),
)

index_job = define_asset_job(
    name="index_job",
    selection=AssetSelection.assets(index_ohlcv),
)

macro_job = define_asset_job(
    name="macro_job",
    selection=AssetSelection.assets(macro_ohlcv),
)

_STOCK_PRODUCT_SELECTION = _STOCK_REFRESH_SELECTION | AssetSelection.assets(portfolio_ranker_top_20_log)

warehouse_refresh_job = define_asset_job(
    name="warehouse_refresh_job",
    selection=_STOCK_PRODUCT_SELECTION,
)

snapshot_job = define_asset_job(
    name="snapshot_job",
    selection=AssetSelection.assets(*_DBT_SNAPSHOT_ASSETS),
)

backtest_job = define_asset_job(
    name="backtest_job",
    selection=(
        _STOCK_REFRESH_SELECTION
        | AssetSelection.assets(dbt_backtest_assets, data_quality)
        | AssetSelection.assets(backtest_performance_log, data_quality_report)
    ),
)

sanity_job = define_asset_job(
    name="sanity_job",
    selection=(
        _STOCK_REFRESH_SELECTION
        | AssetSelection.assets(dbt_backtest_assets, data_quality)
        | AssetSelection.assets(portfolio_ranker_top_20_log, backtest_performance_log, data_quality_report)
    ),
)

# Daily at 23:00 UTC on weekdays, after equity/index/macro market closes.
warehouse_refresh_schedule = ScheduleDefinition(
    job=warehouse_refresh_job,
    cron_schedule="0 23 * * 1-5",
)

# Weekly Sunday 02:00 UTC; backtests are intentionally expensive.
backtest_schedule = ScheduleDefinition(
    job=backtest_job,
    cron_schedule="0 2 * * 0",
)
