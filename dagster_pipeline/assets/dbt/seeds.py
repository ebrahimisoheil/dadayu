from __future__ import annotations

from dagster_dbt import DbtCliResource, dbt_assets

from dagster_pipeline.assets.dbt._common import DBT_MANIFEST, DadayuDbtTranslator


@dbt_assets(
    manifest=DBT_MANIFEST,
    select="resource_type:seed",
    dagster_dbt_translator=DadayuDbtTranslator(),
)
def dbt_seed_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["seed"], context=context).stream()
    yield from dbt.cli(
        ["test", "--select", "resource_type:seed", "--exclude", "test_type:singular"],
        context=context,
    ).stream()
