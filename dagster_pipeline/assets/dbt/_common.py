from __future__ import annotations

from pathlib import Path

from dagster import AssetDep, AssetKey
from dagster_dbt import DagsterDbtTranslator

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent / "warehouse"
DBT_MANIFEST = DBT_PROJECT_DIR / "target" / "manifest.json"

_SOURCE_UPSTREAM: dict[str, AssetKey] = {
    "prices_hourly":        AssetKey("equity_ohlcv"),
    "prices_4h":            AssetKey("equity_ohlcv"),
    "prices_daily":         AssetKey("equity_ohlcv"),
    "tickers":              AssetKey("equity_ticker_info"),
    "crypto_prices_hourly": AssetKey("crypto_ohlcv"),
    "crypto_prices_4h":     AssetKey("crypto_ohlcv"),
    "crypto_prices_daily":  AssetKey("crypto_ohlcv"),
    "crypto_metadata":      AssetKey("crypto_info"),
    "polymarket_markets":   AssetKey("polymarket_markets"),
    "polymarket_prices":    AssetKey("polymarket_prices"),
}


class DadayuDbtTranslator(DagsterDbtTranslator):
    def get_asset_spec(self, manifest, unique_id, project=None):
        spec = super().get_asset_spec(manifest, unique_id, project)
        sources = manifest.get("sources", {})
        node = sources.get(unique_id, {})
        if node.get("resource_type") == "source":
            upstream = _SOURCE_UPSTREAM.get(node.get("name", ""))
            if upstream:
                spec = spec.replace_attributes(
                    deps=list(spec.deps) + [AssetDep(upstream)]
                )
        return spec
