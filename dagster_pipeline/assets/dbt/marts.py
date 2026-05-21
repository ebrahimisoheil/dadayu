from __future__ import annotations

from dagster_dbt import DbtCliResource, dbt_assets

from dagster_pipeline.assets.dbt._common import DBT_MANIFEST, DadayuDbtTranslator


@dbt_assets(
    manifest=DBT_MANIFEST,
    exclude="staging resource_type:snapshot resource_type:seed",
    dagster_dbt_translator=DadayuDbtTranslator(),
)
def dbt_mart_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
    yield from dbt.cli(["test"], context=context).stream()
