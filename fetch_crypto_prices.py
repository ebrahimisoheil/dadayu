#!/usr/bin/env python3
"""Fetch crypto OHLCV prices → ClickHouse.

Usage:
  python fetch_crypto_prices.py --interval 1d
  python fetch_crypto_prices.py --interval 1h --start 2026-03-01 --end 2026-05-19
"""

from __future__ import annotations

import argparse

import pandas as pd

from dadayu.db import get_ch_client
from dadayu.insert import insert_ohlcv
from dadayu.ingest.crypto import INTERVAL_TABLE, download_ohlcv, load_symbols
from dadayu.watermark import get_watermark


def last_month_range() -> tuple[str, str]:
    today = pd.Timestamp.today()
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - pd.Timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    return last_month_start.strftime("%Y-%m-%d"), last_month_end.strftime("%Y-%m-%d")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch crypto OHLCV prices → ClickHouse")
    parser.add_argument("--interval", choices=list(INTERVAL_TABLE.keys()), default="1d")
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()

    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    client = get_ch_client()

    if args.start:
        start = args.start
    else:
        table = INTERVAL_TABLE[args.interval]
        date_col = "date" if args.interval == "1d" else "datetime"
        watermark = get_watermark(client, table, date_col)
        if watermark:
            start = watermark
            print(f"  Watermark: resuming from {start}")
        else:
            start, _ = last_month_range()
            print(f"  No watermark — defaulting to {start}")

    end = args.end or today
    print(f"\n=== CRYPTO | {args.interval} | {start} → {end} ===")

    symbols = load_symbols()
    print(f"  Loaded {len(symbols)} symbols")

    prices = download_ohlcv(symbols, start, end, args.interval)
    if prices.empty:
        print("  [ERROR] No data — aborting.")
        return

    insert_ohlcv(client, INTERVAL_TABLE[args.interval], prices, "crypto", args.interval)
    print("\nDone.")


if __name__ == "__main__":
    main()
