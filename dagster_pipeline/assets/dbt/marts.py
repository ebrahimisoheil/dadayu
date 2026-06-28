from __future__ import annotations

from dagster_dbt import DbtCliResource, dbt_assets

from dagster_pipeline.assets.dbt._common import DBT_MANIFEST, DadayuDbtTranslator


@dbt_assets(
    manifest=DBT_MANIFEST,
    select="02_intermediate 03_marts",
    exclude="03_marts.backtests int_market_backtest_prices_daily int_market_forward_prices_daily int_backtest_asset_scores_daily",
    dagster_dbt_translator=DadayuDbtTranslator(),
)
def dbt_mart_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
    yield from dbt.cli(["test"], context=context).stream()
