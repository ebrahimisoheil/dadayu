from __future__ import annotations

from dagster import Definitions
from dagster_dbt import DbtCliResource

from dagster_pipeline.assets.backtests import backtest_performance_log
from dagster_pipeline.assets.dbt import (
    data_quality,
    dbt_backtest_assets,
    dbt_mart_assets,
    dbt_seed_assets,
    dbt_snapshot_assets,
    dbt_staging_assets,
)
from dagster_pipeline.assets.dbt._common import DBT_PROJECT_DIR
from dagster_pipeline.assets.equity import equity_index_membership, equity_ohlcv, equity_ticker_info
from dagster_pipeline.assets.indexes import index_ohlcv
from dagster_pipeline.assets.macro import macro_ohlcv
from dagster_pipeline.assets.product import portfolio_ranker_top_20_log
from dagster_pipeline.assets.reports import data_quality_report
from dagster_pipeline.resources import PostgresResource
from dagster_pipeline.schedules import (
    backtest_job,
    backtest_schedule,
    equity_job,
    index_job,
    macro_job,
    sanity_job,
    snapshot_job,
    warehouse_refresh_job,
    warehouse_refresh_schedule,
)

defs = Definitions(
    assets=[
        equity_ohlcv,
        equity_ticker_info,
        equity_index_membership,
        index_ohlcv,
        macro_ohlcv,
        dbt_seed_assets,
        dbt_staging_assets,
        dbt_snapshot_assets,
        dbt_mart_assets,
        dbt_backtest_assets,
        data_quality,
        portfolio_ranker_top_20_log,
        backtest_performance_log,
        data_quality_report,
    ],
    resources={
        "postgres": PostgresResource(),
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
    jobs=[
        equity_job,
        index_job,
        macro_job,
        warehouse_refresh_job,
        snapshot_job,
        backtest_job,
        sanity_job,
    ],
    schedules=[
        warehouse_refresh_schedule,
        backtest_schedule,
    ],
)
