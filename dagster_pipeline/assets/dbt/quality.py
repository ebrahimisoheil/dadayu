from __future__ import annotations

from dagster import AssetKey, Failure, MetadataValue, Output, asset

from dadayu.checks import CheckResult, run_all_checks
from dagster_pipeline.resources import ClickhouseResource


@asset(
    group_name="quality",
    deps=[
        AssetKey("fct_ohlcv_1h"),
        AssetKey("fct_indicators_1d"),
        AssetKey("fct_polymarket_signals"),
    ],
)
def data_quality(clickhouse: ClickhouseResource) -> Output:
    client = clickhouse.get_client()
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
