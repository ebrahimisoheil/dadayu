from __future__ import annotations

from dagster import AssetKey, Failure, MetadataValue, Output, asset

from dadayu.checks import CheckResult, run_all_checks
from dagster_pipeline.resources import PostgresResource


@asset(
    group_name="quality",
    deps=[
        AssetKey("int_market_data_quality_daily"),
        AssetKey("mart_portfolio_asset_scores_daily"),
        AssetKey("mart_macro_regime_daily"),
        AssetKey("mart_universe_health_current"),
        AssetKey("mart_product_top_lists_current"),
        AssetKey("mart_backtest_trades"),
        AssetKey("mart_backtest_performance"),
    ],
)
def data_quality(postgres: PostgresResource) -> Output:
    client = postgres.get_client()
    results = run_all_checks(client)

    fail_count = sum(1 for r in results if r.status == "FAIL")
    warn_count = sum(1 for r in results if r.status == "WARN")
    pass_count = sum(1 for r in results if r.status == "PASS")

    failures = [f"{r.section}/{r.name}: {r.value} — {r.detail}" for r in results if r.status == "FAIL"]
    warnings = [f"{r.section}/{r.name}: {r.value} — {r.detail}" for r in results if r.status == "WARN"]

    metadata = {
        "pass_count": MetadataValue.int(pass_count),
        "warn_count": MetadataValue.int(warn_count),
        "fail_count": MetadataValue.int(fail_count),
        "warnings": MetadataValue.text("\n".join(warnings) if warnings else "none"),
    }

    if fail_count > 0:
        raise Failure(
            description=f"{fail_count} data quality check(s) failed",
            metadata={**metadata, "failures": MetadataValue.text("\n".join(failures))},
        )

    return Output(value=None, metadata=metadata)
