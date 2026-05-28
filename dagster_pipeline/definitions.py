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
