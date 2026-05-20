from __future__ import annotations

import time
from datetime import datetime, timezone

import pandas as pd
from dagster import asset

from dadayu.ingest.polymarket import discover_markets, fetch_price_history
from dagster_pipeline.resources import ClickhouseResource


def _ninety_days_ago_ts() -> int:
    return int((datetime.now(timezone.utc) - pd.Timedelta(days=90)).timestamp())


@asset(group_name="ingestion")
def polymarket_markets(clickhouse: ClickhouseResource) -> None:
    client = clickhouse.get_client()
    df = discover_markets(min_volume_usd=50_000)
    if df.empty:
        print("  No markets discovered.")
        return
    df["fetched_at"] = datetime.now(timezone.utc)
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

    # Safety: don't tombstone if Gamma returned suspiciously few markets
    if len(df) == 0:
        print("  Gamma returned 0 markets — skipping tombstone check to avoid mass-close")
    elif len(newly_closed) > 0.5 * len(prev_active) and len(prev_active) > 10:
        print(f"  Skipping tombstone: {len(newly_closed)}/{len(prev_active)} markets would be closed — likely Gamma API issue")
    elif newly_closed:
        now = datetime.now(timezone.utc)
        # Fetch existing metadata to preserve it in tombstone rows
        placeholders = ", ".join(f"'{cid}'" for cid in newly_closed)
        meta_result = client.query(
            f"SELECT condition_id, question, category, yes_token_id "
            f"FROM polymarket_markets FINAL "
            f"WHERE condition_id IN ({placeholders})"
        )
        meta_map = {row[0]: (row[1], row[2], row[3]) for row in meta_result.result_rows}
        tombstone_rows = pd.DataFrame([{
            "condition_id": cid,
            "question": meta_map.get(cid, ("", "", ""))[0],
            "category": meta_map.get(cid, ("", "", ""))[1],
            "volume_usd": 0.0,
            "liquidity_usd": 0.0,
            "active": False,
            "closed": True,
            "resolution_date": None,
            "outcome": None,
            "yes_token_id": meta_map.get(cid, ("", "", ""))[2],
            "linked_asset": None,
            "asset_type": None,
            "fetched_at": now,
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

    now_ts = int(datetime.now(timezone.utc).timestamp())
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
            start_ts = int((pd.Timestamp(max_ts_val, tz="UTC") + pd.Timedelta(hours=1)).timestamp())

        if start_ts >= now_ts:
            continue

        try:
            df = fetch_price_history(yes_token_id, start_ts, now_ts)
            if not df.empty:
                df["condition_id"] = condition_id
                df["ingested_at"] = datetime.now(timezone.utc)
                client.insert_df(
                    "polymarket_prices",
                    df[["condition_id", "ts", "probability", "volume_usd", "ingested_at"]],
                )
                total_rows += len(df)
        except Exception as exc:
            print(f"  [WARN] Failed prices for {condition_id}: {exc}")

        time.sleep(0.15)

    print(f"  Inserted {total_rows:,} rows for {len(markets)} markets")
