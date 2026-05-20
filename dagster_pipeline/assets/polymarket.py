from __future__ import annotations

import time

import pandas as pd
from dagster import asset

from dadayu.ingest.polymarket import discover_markets, fetch_price_history
from dadayu.watermark import get_watermark
from dagster_pipeline.resources import ClickhouseResource


def _ninety_days_ago_ts() -> int:
    return int((pd.Timestamp.now() - pd.Timedelta(days=90)).timestamp())


@asset(group_name="ingestion")
def polymarket_markets(clickhouse: ClickhouseResource) -> None:
    client = clickhouse.get_client()
    df = discover_markets(min_volume_usd=50_000)
    if df.empty:
        print("  No markets discovered.")
        return
    df["fetched_at"] = pd.Timestamp.now()
    cols = [
        "condition_id", "question", "category", "volume_usd", "liquidity_usd",
        "active", "closed", "resolution_date", "outcome", "yes_token_id",
        "linked_asset", "asset_type", "fetched_at",
    ]
    client.insert_df("polymarket_markets", df[cols])
    print(f"  Upserted {len(df):,} markets into polymarket_markets")


@asset(group_name="ingestion", deps=[polymarket_markets])
def polymarket_prices(clickhouse: ClickhouseResource) -> None:
    client = clickhouse.get_client()

    result = client.query(
        "SELECT condition_id, yes_token_id FROM polymarket_markets FINAL WHERE closed = false"
    )
    markets = result.result_rows
    if not markets:
        print("  No active markets found — run polymarket_markets first.")
        return

    now_ts = int(pd.Timestamp.now().timestamp())
    fallback_start = _ninety_days_ago_ts()
    total_rows = 0

    for condition_id, yes_token_id in markets:
        watermark_str = get_watermark(
            client, "polymarket_prices", "ts", condition_id=condition_id
        )
        start_ts = (
            int(pd.Timestamp(watermark_str).timestamp())
            if watermark_str is not None
            else fallback_start
        )

        if start_ts >= now_ts:
            continue

        try:
            df = fetch_price_history(yes_token_id, start_ts, now_ts)
            if not df.empty:
                df["condition_id"] = condition_id
                df["ingested_at"] = pd.Timestamp.now()
                client.insert_df(
                    "polymarket_prices",
                    df[["condition_id", "ts", "probability", "volume_usd", "ingested_at"]],
                )
                total_rows += len(df)
        except Exception as exc:
            print(f"  [WARN] Failed prices for {condition_id}: {exc}")

        time.sleep(0.15)

    print(f"  Inserted {total_rows:,} rows for {len(markets)} markets")
