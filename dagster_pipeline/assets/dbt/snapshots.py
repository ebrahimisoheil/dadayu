from __future__ import annotations

from dagster_dbt import DbtCliResource, dbt_assets

from dagster_pipeline.assets.dbt._common import DBT_MANIFEST, DadayuDbtTranslator


@dbt_assets(
    manifest=DBT_MANIFEST,
    select="resource_type:snapshot",
    dagster_dbt_translator=DadayuDbtTranslator(),
)
def dbt_snapshot_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["snapshot"], context=context).stream()
    yield from dbt.cli(["test"], context=context).stream()
