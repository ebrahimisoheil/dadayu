#!/usr/bin/env python3
"""Fetch yfinance ticker metadata and append to ClickHouse tickers table.

Always appends — never truncates. ReplacingMergeTree(fetched_at) deduplicates.
Run after any universe change to keep metadata fresh.

Usage:
  python fetch_ticker_info.py                  # all markets
  python fetch_ticker_info.py --market us
"""

from __future__ import annotations

import argparse
import os
import time

import clickhouse_connect
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

MARKETS = ["germany", "us", "india"]


def get_ch_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        database=os.environ.get("CLICKHOUSE_DB", "dadayu"),
        username=os.environ.get("CLICKHOUSE_USER", "dadayu"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


YFINANCE_FIELDS = {
    "longName":      "name",
    "sector":        "sector",
    "industry":      "industry",
    "currency":      "currency",
    "country":       "country",
    "marketCap":     "market_cap",
    "trailingPE":    "pe_ratio",
}


def fetch_market_tickers(market: str) -> list[str]:
    client = get_ch_client()
    result = client.query(
        "SELECT DISTINCT ticker FROM prices_daily WHERE market = {market:String}",
        parameters={"market": market},
    )
    return [row[0] for row in result.result_rows]


def fetch_metadata(tickers: list[str], market: str) -> pd.DataFrame:
    records = []
    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i}/{len(tickers)}] {ticker}", end="\r")
        row = {"ticker": ticker, "market": market}
        try:
            info = yf.Ticker(ticker).info
            for yf_key, col in YFINANCE_FIELDS.items():
                row[col] = info.get(yf_key)
        except Exception as exc:
            print(f"\n  [WARN] {ticker}: {exc}")
            for col in YFINANCE_FIELDS.values():
                row.setdefault(col, None)
        records.append(row)
        time.sleep(0.1)  # rate limit
    print()
    return pd.DataFrame(records)


def insert_metadata(df: pd.DataFrame) -> None:
    client = get_ch_client()
    df = df.copy()
    df["fetched_at"] = pd.Timestamp.now()

    # Ensure correct dtypes
    for col in ["name", "sector", "industry", "currency", "country"]:
        df[col] = df[col].fillna("").astype(str)
    for col in ["market_cap", "pe_ratio"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    cols = ["ticker", "market", "name", "sector", "industry",
            "currency", "country", "market_cap", "pe_ratio", "fetched_at"]
    client.insert_df("tickers", df[cols])
    print(f"  Inserted {len(df)} rows into tickers [{df['market'].iloc[0]}]")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch ticker metadata → ClickHouse tickers")
    parser.add_argument("--market", choices=MARKETS + ["all"], default="all")
    args = parser.parse_args()

    markets = MARKETS if args.market == "all" else [args.market]
    for market in markets:
        print(f"\n=== {market.upper()} — fetching metadata ===")
        tickers = fetch_market_tickers(market)
        if not tickers:
            print(f"  [ERROR] No tickers found for {market} in prices_daily. Run fetch_hourly_prices.py first.")
            continue
        print(f"  Found {len(tickers)} tickers")
        df = fetch_metadata(tickers, market)
        insert_metadata(df)

    print("\nDone.")


if __name__ == "__main__":
    main()
