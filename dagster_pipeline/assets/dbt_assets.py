from __future__ import annotations

from pathlib import Path

from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "warehouse"
DBT_MANIFEST = DBT_PROJECT_DIR / "target" / "manifest.json"

# Maps each dbt source table name to the *upstream* Dagster ingestion asset key.
# Multiple dbt sources can reference the same ingestion asset (e.g. hourly/4h/daily
# timeframes all originate from one equity_ohlcv writer).  Dagster-dbt requires
# every source to have a *unique* AssetKey, so we use a two-segment key:
#   [<upstream_asset_name>, <dbt_source_name>]
# This preserves the logical dependency on the upstream asset while keeping keys
# distinct.  The ingestion assets must declare these same keys via `outs=` or be
# registered as `SourceAsset`s for the lineage to connect in the Dagster UI.
_SOURCE_UPSTREAM: dict[str, str] = {
    "prices_hourly":        "equity_ohlcv",
    "prices_4h":            "equity_ohlcv",
    "prices_daily":         "equity_ohlcv",
    "tickers":              "equity_ticker_info",
    "crypto_prices_hourly": "crypto_ohlcv",
    "crypto_prices_4h":     "crypto_ohlcv",
    "crypto_prices_daily":  "crypto_ohlcv",
    "crypto_metadata":      "crypto_info",
}


class _DadayuDbtTranslator(DagsterDbtTranslator):
    """Custom translator that maps dbt sources to namespaced Dagster asset keys.

    Each dbt source gets a two-segment key ``[<upstream_asset>, <source_table>]``
    so that:
    - Keys remain unique (required by dagster-dbt).
    - The first segment signals which ingestion asset feeds this source.
    """

    def get_asset_key(self, dbt_resource_props: dict) -> AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            name = dbt_resource_props["name"]
            upstream = _SOURCE_UPSTREAM.get(name)
            if upstream:
                return AssetKey([upstream, name])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=DBT_MANIFEST,
    dagster_dbt_translator=_DadayuDbtTranslator(),
)
def dadayu_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
    yield from dbt.cli(["test"], context=context).stream()
