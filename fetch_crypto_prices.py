#!/usr/bin/env python3
"""Fetch OHLCV for top-20 crypto assets and store in ClickHouse crypto_prices_*.

Usage:
  python fetch_crypto_prices.py --interval 1d
  python fetch_crypto_prices.py --interval 1h --start 2026-03-01 --end 2026-05-16
"""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

import clickhouse_connect
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

UNIVERSE_CSV = Path(__file__).parent / "warehouse" / "seeds" / "crypto_universe.csv"

INTERVAL_TABLE = {
    "1h": "crypto_prices_hourly",
    "4h": "crypto_prices_4h",
    "1d": "crypto_prices_daily",
}


def get_ch_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        database=os.environ.get("CLICKHOUSE_DB", "dadayu"),
        username=os.environ.get("CLICKHOUSE_USER", "dadayu"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


def load_symbols() -> list[str]:
    with open(UNIVERSE_CSV, newline="") as f:
        return [row["symbol"] for row in csv.DictReader(f)]


def get_watermark(client: clickhouse_connect.driver.Client, interval: str) -> str | None:
    table = INTERVAL_TABLE[interval]
    date_col = "date" if interval == "1d" else "datetime"
    try:
        result = client.query(f"SELECT max({date_col}) FROM {table}")
        val = result.result_rows[0][0]
        if val is None:
            return None
        return (pd.Timestamp(val) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception as exc:
        print(f"  [WARN] Watermark query failed: {exc}")
        return None


def download_prices(symbols: list[str], start: str, end: str, interval: str) -> pd.DataFrame:
    print(f"  Downloading {interval} data {start} → {end} for {len(symbols)} symbols...")
    end_exclusive = (pd.Timestamp(end) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    raw = yf.download(
        tickers=symbols,
        start=start,
        end=end_exclusive,
        interval=interval,
        auto_adjust=True,
        progress=True,
        group_by="ticker",
        threads=True,
    )

    if raw.empty:
        print("  [WARN] No data returned.")
        return pd.DataFrame()

    rows: list[pd.DataFrame] = []
    for symbol in symbols:
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                if symbol not in raw.columns.get_level_values(0):
                    continue
                df = raw[symbol].copy()
            else:
                df = raw.copy()

            df = df.dropna(subset=["Close"])
            if df.empty:
                continue

            df.index = pd.to_datetime(df.index).tz_localize(None)
            df = df.reset_index().rename(columns={
                "index": "Datetime", "Datetime": "Datetime",
                "Date": "Datetime", "Price": "Datetime",
            })
            df["Ticker"] = symbol
            keep = ["Datetime", "Ticker", "Open", "High", "Low", "Close", "Volume"]
            rows.append(df[[c for c in keep if c in df.columns]])
        except Exception as exc:
            print(f"  [WARN] Parse failed for {symbol}: {exc}")

    if not rows:
        return pd.DataFrame()

    prices = pd.concat(rows, ignore_index=True)
    prices = prices.sort_values(["Ticker", "Datetime"]).reset_index(drop=True)
    print(f"  Got {len(prices):,} rows for {prices['Ticker'].nunique()} symbols")
    return prices


def insert_prices(client: clickhouse_connect.driver.Client, prices: pd.DataFrame, interval: str) -> None:
    table = INTERVAL_TABLE[interval]
    is_daily = interval == "1d"
    df = prices.copy()
    df["market"] = "crypto"

    if is_daily:
        df["Datetime"] = pd.to_datetime(df["Datetime"]).dt.date
        df = df.rename(columns={
            "Ticker": "ticker", "Datetime": "date",
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        cols = ["ticker", "market", "date", "open", "high", "low", "close", "volume"]
        df["_ym"] = pd.to_datetime(df["date"]).dt.to_period("M")
    else:
        df["Datetime"] = pd.to_datetime(df["Datetime"]).dt.tz_localize(None)
        df = df.rename(columns={
            "Ticker": "ticker", "Datetime": "datetime",
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        cols = ["ticker", "market", "datetime", "open", "high", "low", "close", "volume"]
        df["_ym"] = df["datetime"].dt.to_period("M")

    df = df[[c for c in cols if c in df.columns] + ["_ym"]].dropna(subset=["close"])
    df["volume"] = df["volume"].fillna(0).clip(lower=0).astype(int)

    total = 0
    for _, chunk in df.groupby("_ym"):
        client.insert_df(table, chunk.drop(columns=["_ym"]))
        total += len(chunk)
    print(f"  Inserted {total:,} rows into {table}")


def last_month_range() -> tuple[str, str]:
    today = pd.Timestamp.today()
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - pd.Timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    return last_month_start.strftime("%Y-%m-%d"), last_month_end.strftime("%Y-%m-%d")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch crypto OHLCV → ClickHouse")
    parser.add_argument("--interval", choices=list(INTERVAL_TABLE.keys()), default="1d")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   help="End date YYYY-MM-DD")
    args = parser.parse_args()

    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    client = get_ch_client()

    if args.start:
        start = args.start
    else:
        watermark = get_watermark(client, args.interval)
        if watermark:
            start = watermark
            print(f"  Watermark: resuming from {start}")
        else:
            start, _ = last_month_range()
            print(f"  No watermark — defaulting to {start}")

    end = args.end or today

    print(f"\n=== CRYPTO | {args.interval} | {start} → {end} ===")
    symbols = load_symbols()
    print(f"  Loaded {len(symbols)} symbols from crypto_universe.csv")

    prices = download_prices(symbols, start, end, args.interval)
    if prices.empty:
        print("  [ERROR] No data — aborting.")
        return

    insert_prices(client, prices, args.interval)
    print("\nDone.")


if __name__ == "__main__":
    main()
