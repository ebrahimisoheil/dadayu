from __future__ import annotations

import pandas as pd
from dagster import asset

from dadayu.ingest.equity import download_ohlcv
from dadayu.ingest.indexes import INTERVAL_TABLE, MARKET, load_symbols
from dadayu.insert import insert_ohlcv
from dadayu.watermark import get_watermark
from dagster_pipeline.resources import PostgresResource


def _five_year_backfill_start() -> str:
    return (pd.Timestamp.today() - pd.DateOffset(years=5)).strftime("%Y-%m-%d")


@asset(group_name="ingestion")
def index_ohlcv(postgres: PostgresResource) -> None:
    client = postgres.get_client()
    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    symbols = load_symbols()
    for interval, table in INTERVAL_TABLE.items():
        start = get_watermark(client, table, "date", market=MARKET) or _five_year_backfill_start()
        if pd.Timestamp(start) > pd.Timestamp(today):
            print(f"  {table} [{MARKET}] is already current through {today}")
            continue
        prices = download_ohlcv(symbols, start, today, interval)
        if prices.empty:
            continue
        insert_ohlcv(client, table, prices, MARKET, interval)
