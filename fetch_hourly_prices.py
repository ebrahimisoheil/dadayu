#!/usr/bin/env python3
"""Fetch equity OHLCV prices → ClickHouse.

Usage:
  python fetch_hourly_prices.py                    # all markets, watermark or last month
  python fetch_hourly_prices.py --market us        # single market
  python fetch_hourly_prices.py --interval 1d --start 2026-03-01 --end 2026-05-19
"""

from __future__ import annotations

import argparse

import pandas as pd

from dadayu.db import get_ch_client
from dadayu.insert import insert_ohlcv
from dadayu.ingest.equity import INTERVAL_TABLE, MARKETS, download_ohlcv, get_tickers
from dadayu.watermark import get_watermark


def last_month_range() -> tuple[str, str]:
    today = pd.Timestamp.today()
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - pd.Timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    return last_month_start.strftime("%Y-%m-%d"), last_month_end.strftime("%Y-%m-%d")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch equity OHLCV prices → ClickHouse")
    parser.add_argument("--market", choices=MARKETS + ["all"], default="all")
    parser.add_argument("--interval", choices=list(INTERVAL_TABLE.keys()), default="1h")
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()

    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    markets = MARKETS if args.market == "all" else [args.market]
    client = get_ch_client()

    for market in markets:
        if args.start:
            start = args.start
        else:
            table = INTERVAL_TABLE[args.interval]
            date_col = "date" if args.interval == "1d" else "datetime"
            watermark = get_watermark(client, table, date_col, market=market)
            if watermark:
                start = watermark
                print(f"  [{market}] Watermark: resuming from {start}")
            else:
                start, _ = last_month_range()
                print(f"  [{market}] No watermark — defaulting to {start}")

        end = args.end or today
        print(f"\n=== {market.upper()} | {args.interval} | {start} → {end} ===")

        tickers = get_tickers(market)
        if not tickers:
            print(f"  [ERROR] No tickers for {market} — skipping.")
            continue

        prices = download_ohlcv(tickers, start, end, args.interval)
        if prices.empty:
            print(f"  [ERROR] No price data for {market}.")
            continue

        insert_ohlcv(client, INTERVAL_TABLE[args.interval], prices, market, args.interval)

    client.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
