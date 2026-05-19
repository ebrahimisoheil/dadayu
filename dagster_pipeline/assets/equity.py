from __future__ import annotations

import pandas as pd
from dagster import asset

from dadayu.ingest.equity import (
    INTERVAL_TABLE,
    MARKETS,
    download_ohlcv,
    fetch_ticker_metadata,
    get_tickers,
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
def equity_ohlcv(clickhouse: ClickhouseResource) -> None:
    client = clickhouse.get_client()
    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    for market in MARKETS:
        for interval, table in INTERVAL_TABLE.items():
            date_col = "date" if interval == "1d" else "datetime"
            start = get_watermark(client, table, date_col, market=market) or _last_month_start()
            tickers = get_tickers(market)
            if not tickers:
                continue
            prices = download_ohlcv(tickers, start, today, interval)
            if prices.empty:
                continue
            insert_ohlcv(client, table, prices, market, interval)


@asset(group_name="ingestion", deps=[equity_ohlcv])
def equity_ticker_info(clickhouse: ClickhouseResource) -> None:
    client = clickhouse.get_client()
    for market in MARKETS:
        result = client.query(
            "SELECT DISTINCT ticker FROM prices_daily WHERE market = {market:String}",
            parameters={"market": market},
        )
        tickers = [row[0] for row in result.result_rows]
        if not tickers:
            continue
        df = fetch_ticker_metadata(tickers, market)
        df["fetched_at"] = pd.Timestamp.now()
        for col in ["name", "sector", "industry", "currency", "country"]:
            df[col] = df[col].fillna("").astype(str)
        for col in ["market_cap", "pe_ratio"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        cols = ["ticker", "market", "name", "sector", "industry",
                "currency", "country", "market_cap", "pe_ratio", "fetched_at"]
        client.insert_df("tickers", df[cols])
