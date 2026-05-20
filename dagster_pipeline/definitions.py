from __future__ import annotations

from dagster import Definitions
from dagster_dbt import DbtCliResource

from dagster_pipeline.assets.crypto import crypto_info, crypto_ohlcv
from dagster_pipeline.assets.dbt_assets import DBT_PROJECT_DIR, dadayu_dbt_assets
from dagster_pipeline.assets.equity import equity_ohlcv, equity_ticker_info
from dagster_pipeline.assets.polymarket import polymarket_markets, polymarket_prices
from dagster_pipeline.resources import ClickhouseResource
from dagster_pipeline.schedules import (
    crypto_job,
    crypto_schedule,
    equity_job,
    equity_schedule,
    polymarket_job,
    polymarket_schedule,
)

defs = Definitions(
    assets=[
        equity_ohlcv,
        equity_ticker_info,
        crypto_ohlcv,
        crypto_info,
        polymarket_markets,
        polymarket_prices,
        dadayu_dbt_assets,
    ],
    resources={
        "clickhouse": ClickhouseResource(),
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
    jobs=[equity_job, crypto_job, polymarket_job],
    schedules=[equity_schedule, crypto_schedule, polymarket_schedule],
)
