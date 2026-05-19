#!/usr/bin/env python3
"""Fetch yfinance ticker metadata → ClickHouse tickers.

Usage:
  python fetch_ticker_info.py
  python fetch_ticker_info.py --market us
"""

from __future__ import annotations

import argparse

import pandas as pd

from dadayu.db import get_ch_client
from dadayu.ingest.equity import MARKETS, fetch_ticker_metadata


def fetch_market_tickers(market: str) -> list[str]:
    client = get_ch_client()
    result = client.query(
        "SELECT DISTINCT ticker FROM prices_daily WHERE market = {market:String}",
        parameters={"market": market},
    )
    return [row[0] for row in result.result_rows]


def insert_metadata(df: pd.DataFrame) -> None:
    client = get_ch_client()
    data = df.copy()
    data["fetched_at"] = pd.Timestamp.now()
    for col in ["name", "sector", "industry", "currency", "country"]:
        data[col] = data[col].fillna("").astype(str)
    for col in ["market_cap", "pe_ratio"]:
        data[col] = pd.to_numeric(data[col], errors="coerce")
    cols = ["ticker", "market", "name", "sector", "industry",
            "currency", "country", "market_cap", "pe_ratio", "fetched_at"]
    client.insert_df("tickers", data[cols])
    print(f"  Inserted {len(data)} rows into tickers [{data['market'].iloc[0]}]")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch ticker metadata → ClickHouse tickers")
    parser.add_argument("--market", choices=MARKETS + ["all"], default="all")
    args = parser.parse_args()

    markets = MARKETS if args.market == "all" else [args.market]
    for market in markets:
        print(f"\n=== {market.upper()} — fetching metadata ===")
        tickers = fetch_market_tickers(market)
        if not tickers:
            print(f"  [ERROR] No tickers for {market} in prices_daily. Run fetch_hourly_prices.py first.")
            continue
        print(f"  Found {len(tickers)} tickers")
        df = fetch_ticker_metadata(tickers, market)
        insert_metadata(df)

    print("\nDone.")


if __name__ == "__main__":
    main()
