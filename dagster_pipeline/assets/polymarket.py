from __future__ import annotations

import time

import pandas as pd
from dagster import asset

from dadayu.ingest.polymarket import discover_markets, fetch_price_history
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

    # Tombstone markets that disappeared from Gamma (implicitly closed)
    result = client.query(
        "SELECT condition_id FROM polymarket_markets FINAL WHERE closed = false"
    )
    prev_active = {row[0] for row in result.result_rows}
    newly_closed = prev_active - set(df["condition_id"].tolist())
    if newly_closed:
        now = pd.Timestamp.now()
        tombstone_rows = pd.DataFrame([{
            "condition_id": cid, "question": "", "category": "",
            "volume_usd": 0.0, "liquidity_usd": 0.0,
            "active": False, "closed": True,
            "resolution_date": None, "outcome": None, "yes_token_id": "",
            "linked_asset": None, "asset_type": None, "fetched_at": now,
        } for cid in newly_closed])
        client.insert_df("polymarket_markets", tombstone_rows[cols])
        print(f"  Marked {len(newly_closed)} markets as closed (no longer in Gamma response)")


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
        wm_result = client.query(
            "SELECT max(ts) FROM polymarket_prices WHERE condition_id = {condition_id:String}",
            parameters={"condition_id": condition_id},
        )
        max_ts_val = wm_result.result_rows[0][0]
        if max_ts_val is None:
            start_ts = fallback_start
        else:
            start_ts = int((pd.Timestamp(max_ts_val) + pd.Timedelta(hours=1)).timestamp())

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
