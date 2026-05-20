from __future__ import annotations

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

from dagster_pipeline.assets.crypto import crypto_info, crypto_ohlcv
from dagster_pipeline.assets.dbt_assets import dadayu_dbt_assets
from dagster_pipeline.assets.equity import equity_ohlcv, equity_ticker_info
from dagster_pipeline.assets.polymarket import polymarket_markets, polymarket_prices

equity_job = define_asset_job(
    name="equity_job",
    selection=AssetSelection.assets(
        equity_ohlcv, equity_ticker_info, dadayu_dbt_assets
    ),
)

crypto_job = define_asset_job(
    name="crypto_job",
    selection=AssetSelection.assets(
        crypto_ohlcv, crypto_info, dadayu_dbt_assets
    ),
)

polymarket_job = define_asset_job(
    name="polymarket_job",
    selection=AssetSelection.assets(
        polymarket_markets, polymarket_prices
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

# Every 4 hours, 24/7 (prediction markets never close)
polymarket_schedule = ScheduleDefinition(
    job=polymarket_job,
    cron_schedule="0 */4 * * *",
)
