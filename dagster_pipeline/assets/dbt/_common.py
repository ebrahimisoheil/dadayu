from __future__ import annotations

from pathlib import Path

from dagster import AssetDep, AssetKey
from dagster_dbt import DagsterDbtTranslator

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent / "warehouse"
DBT_MANIFEST = DBT_PROJECT_DIR / "target" / "manifest.json"

# Maps dbt source *name* → Dagster ingestion asset that populates it.
# Used by DadayuDbtTranslator to inject ingestion deps onto model/snapshot specs.
# We trace each model's depends_on.nodes → source → ingestion key, so multiple
# sources can point to the same ingestion asset without key-uniqueness collisions.
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

        # For model/snapshot nodes: walk their source deps and inject ingestion
        # asset keys as explicit Dagster deps. This avoids modifying source
        # asset keys (which would cause key-uniqueness validation failures when
        # multiple sources map to the same ingestion asset).
        all_nodes = {**manifest.get("nodes", {}), **manifest.get("snapshots", {})}
        node = all_nodes.get(unique_id, {})
        if node.get("resource_type") in ("model", "snapshot"):
            extra: set[AssetKey] = set()
            for dep_id in node.get("depends_on", {}).get("nodes", []):
                if dep_id.startswith("source."):
                    source_name = manifest.get("sources", {}).get(dep_id, {}).get("name", "")
                    upstream = _SOURCE_UPSTREAM.get(source_name)
                    if upstream:
                        extra.add(upstream)
            if extra:
                existing = {d.asset_key for d in spec.deps}
                new_deps = [AssetDep(k) for k in extra if k not in existing]
                if new_deps:
                    spec = spec.replace_attributes(deps=list(spec.deps) + new_deps)

        return spec
