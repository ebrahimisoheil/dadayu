from __future__ import annotations

import pandas as pd
from dagster import asset

from dadayu.ingest.crypto import (
    INTERVAL_TABLE,
    build_metadata,
    download_ohlcv,
    fetch_coingecko_markets,
    load_symbols,
    load_universe,
)
from dadayu.insert import insert_ohlcv
from dadayu.watermark import get_watermark
from dagster_pipeline.resources import ClickhouseResource


def _last_month_start() -> str:
    today = pd.Timestamp.today()
    first = today.replace(day=1)
    last_month_end = first - pd.Timedelta(days=1)
    return last_month_end.replace(day=1).strftime("%Y-%m-%d")


@asset(group_name="ingestion")
def crypto_ohlcv(clickhouse: ClickhouseResource) -> None:
    client = clickhouse.get_client()
    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    symbols = load_symbols()
    for interval, table in INTERVAL_TABLE.items():
        date_col = "date" if interval == "1d" else "datetime"
        start = get_watermark(client, table, date_col) or _last_month_start()
        prices = download_ohlcv(symbols, start, today, interval)
        if prices.empty:
            continue
        insert_ohlcv(client, table, prices, "crypto", interval)


@asset(group_name="ingestion", deps=[crypto_ohlcv])
def crypto_info(clickhouse: ClickhouseResource) -> None:
    client = clickhouse.get_client()
    universe = load_universe()
    coingecko_ids = [row["coingecko_id"] for row in universe]
    markets_data = fetch_coingecko_markets(coingecko_ids)
    df = build_metadata(universe, markets_data)
    cols = ["coin_id", "symbol", "name", "rank", "market_cap", "category", "chain", "fetched_at"]
    client.insert_df("crypto_metadata", df[cols])
