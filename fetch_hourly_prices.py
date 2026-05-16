#!/usr/bin/env python3
"""Fetch last-month hourly OHLCV for all 3 markets and store in ClickHouse prices_hourly.

Usage:
  python fetch_hourly_prices.py                    # all markets, last calendar month
  python fetch_hourly_prices.py --market us        # single market
  python fetch_hourly_prices.py --start 2026-03-01 --end 2026-03-31
"""

from __future__ import annotations

import argparse
import os
from io import StringIO
from urllib.request import Request, urlopen

import clickhouse_connect
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

MARKETS = ["germany", "us", "india"]

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
    )
}

# ---------------------------------------------------------------------------
# ClickHouse
# ---------------------------------------------------------------------------

def get_ch_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        database=os.environ.get("CLICKHOUSE_DB", "dadayu"),
        username=os.environ.get("CLICKHOUSE_USER", "dadayu"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


# ---------------------------------------------------------------------------
# Ticker scraping (mirrors test-screener approach)
# ---------------------------------------------------------------------------

def _fetch_html(url: str) -> str:
    req = Request(url, headers=REQUEST_HEADERS)
    with urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8")


def _get_germany_tickers() -> list[str]:
    sources = {
        "DAX":    ["https://en.wikipedia.org/wiki/DAX"],
        "MDAX":   ["https://en.wikipedia.org/wiki/MDAX"],
        "SDAX":   ["https://en.wikipedia.org/wiki/SDAX", "https://ru.wikipedia.org/wiki/SDAX"],
        "TecDAX": ["https://en.wikipedia.org/wiki/TecDAX", "https://de.wikipedia.org/wiki/TecDAX"],
    }
    candidates = ["ticker symbol", "ticker", "symbol", "кратное", "тикер", "abbr.", "abbreviation"]
    tickers: set[str] = set()

    for index, urls in sources.items():
        for url in urls:
            try:
                tables = pd.read_html(StringIO(_fetch_html(url)))
                for table in tables:
                    col_map = {str(c).strip().lower(): c for c in table.columns}
                    found_col = None
                    for cand in candidates:
                        if cand in col_map:
                            found_col = col_map[cand]
                            break
                        for key, orig in col_map.items():
                            if key.startswith(cand) and len(key) < len(cand) + 6:
                                found_col = orig
                                break
                        if found_col:
                            break
                    if found_col is None:
                        continue
                    raw = table[found_col].astype(str).str.strip().str.replace(r"\s+", "", regex=True)
                    raw = raw[raw.str.len() > 1]
                    for t in raw:
                        base = t.replace(".DE", "")
                        if "." in t and not t.endswith(".DE"):
                            tickers.add(t)
                        else:
                            tickers.add(base + ".DE")
                    break
            except Exception as exc:
                print(f"  [WARN] {index} from {url}: {exc}")
                continue
            break

    return sorted(tickers)


def _get_us_tickers() -> list[str]:
    def _convert(t: str) -> str:
        parts = t.split(".")
        return f"{parts[0]}-{parts[1]}" if len(parts) == 2 and len(parts[1]) <= 2 else t

    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        raw = tables[0]["Symbol"].astype(str).str.strip()
        return sorted(_convert(t) for t in raw)
    except Exception as exc:
        print(f"  [WARN] Wikipedia S&P500 failed: {exc} — trying GitHub backup...")

    try:
        import requests
        url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
        raw = df["Symbol"].astype(str).str.strip()
        return sorted(_convert(t) for t in raw)
    except Exception as exc:
        print(f"  [WARN] GitHub S&P500 backup failed: {exc}")
        return []


def _get_india_tickers() -> list[str]:
    url = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"
    try:
        content = _fetch_html(url)
        df = pd.read_csv(StringIO(content))
        if "Symbol" not in df.columns:
            raise ValueError("No Symbol column")
        return sorted(t + ".NS" for t in df["Symbol"].astype(str).str.strip())
    except Exception as exc:
        print(f"  [WARN] NSE CSV failed: {exc} — trying Wikipedia fallback")

    wiki_sources = [
        "https://en.wikipedia.org/wiki/NIFTY_50",
        "https://en.wikipedia.org/wiki/Nifty_Next_50",
    ]
    tickers: set[str] = set()
    for url in wiki_sources:
        try:
            tables = pd.read_html(StringIO(_fetch_html(url)))
            for table in tables:
                col_map = {str(c).strip().lower(): c for c in table.columns}
                if "symbol" in col_map:
                    raw = table[col_map["symbol"]].astype(str).str.strip()
                    for t in raw[raw.str.len() > 1]:
                        tickers.add(t if t.endswith(".NS") else t + ".NS")
                    break
        except Exception as exc:
            print(f"  [WARN] {url}: {exc}")
    return sorted(tickers)


def get_tickers(market: str) -> list[str]:
    print(f"  Fetching tickers for {market}...")
    if market == "germany":
        tickers = _get_germany_tickers()
    elif market == "us":
        tickers = _get_us_tickers()
    elif market == "india":
        tickers = _get_india_tickers()
    else:
        raise ValueError(f"Unknown market: {market}")
    print(f"  Found {len(tickers)} tickers")
    return tickers


# ---------------------------------------------------------------------------
# Hourly download
# ---------------------------------------------------------------------------

INTERVAL_TABLE = {
    "1h": "prices_hourly",
    "4h": "prices_4h",
    "1d": "prices_daily",
}


def download_hourly(tickers: list[str], start: str, end: str, interval: str = "1h") -> pd.DataFrame:
    """Download OHLCV via yfinance. Returns long-format DataFrame."""
    print(f"  Downloading {interval} data {start} → {end} for {len(tickers)} tickers...")

    # yfinance end date is exclusive — add 1 day to include the end date
    end_exclusive = (pd.Timestamp(end) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    raw = yf.download(
        tickers=tickers,
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
    for ticker in tickers:
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                if ticker not in raw.columns.get_level_values(0):
                    continue
                df = raw[ticker].copy()
            else:
                df = raw.copy()

            df = df.dropna(subset=["Close"])
            if df.empty:
                continue

            df.index = pd.to_datetime(df.index).tz_localize(None)
            df = df.reset_index().rename(columns={"index": "Datetime", "Datetime": "Datetime", "Date": "Datetime", "Price": "Datetime"})
            df["Ticker"] = ticker
            keep = ["Datetime", "Ticker", "Open", "High", "Low", "Close", "Volume"]
            rows.append(df[[c for c in keep if c in df.columns]])
        except Exception as exc:
            print(f"  [WARN] Parse failed for {ticker}: {exc}")

    if not rows:
        return pd.DataFrame()

    prices = pd.concat(rows, ignore_index=True)
    prices = prices.sort_values(["Ticker", "Datetime"]).reset_index(drop=True)
    print(f"  Got {len(prices):,} rows for {prices['Ticker'].nunique()} tickers")
    return prices


# ---------------------------------------------------------------------------
# ClickHouse insert
# ---------------------------------------------------------------------------

def insert_prices(
    client: clickhouse_connect.driver.Client,
    prices: pd.DataFrame,
    market: str,
    interval: str,
) -> None:
    table = INTERVAL_TABLE[interval]
    is_daily = interval == "1d"
    df = prices.copy()
    df["market"] = market

    if is_daily:
        df["Datetime"] = pd.to_datetime(df["Datetime"]).dt.date
        df = df.rename(columns={"Ticker": "ticker", "Datetime": "date", "Open": "open",
                                 "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
        cols = ["ticker", "market", "date", "open", "high", "low", "close", "volume"]
        df["_ym"] = pd.to_datetime(df["date"]).dt.to_period("M")
    else:
        df["Datetime"] = pd.to_datetime(df["Datetime"]).dt.tz_localize(None)
        df = df.rename(columns={"Ticker": "ticker", "Datetime": "datetime", "Open": "open",
                                 "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
        cols = ["ticker", "market", "datetime", "open", "high", "low", "close", "volume"]
        df["_ym"] = df["datetime"].dt.to_period("M")

    df = df[[c for c in cols if c in df.columns] + ["_ym"]].dropna(subset=["close"])
    df["volume"] = df["volume"].fillna(0).astype(int)

    total = 0
    for _, chunk in df.groupby("_ym"):
        client.insert_df(table, chunk.drop(columns=["_ym"]))
        total += len(chunk)
    print(f"  Inserted {total:,} rows into {table} [{market}]")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def last_month_range() -> tuple[str, str]:
    today = pd.Timestamp.today()
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - pd.Timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    return last_month_start.strftime("%Y-%m-%d"), last_month_end.strftime("%Y-%m-%d")


def fetch_market(market: str, start: str, end: str, interval: str) -> None:
    print(f"\n=== {market.upper()} | {interval} | {start} → {end} ===")
    tickers = get_tickers(market)
    if not tickers:
        print(f"  [ERROR] No tickers for {market} — skipping.")
        return

    prices = download_hourly(tickers, start, end, interval=interval)
    if prices.empty:
        print(f"  [ERROR] No price data returned for {market}.")
        return

    client = get_ch_client()
    insert_prices(client, prices, market, interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch OHLCV prices → ClickHouse")
    parser.add_argument("--market",   choices=MARKETS + ["all"], default="all")
    parser.add_argument("--interval", choices=list(INTERVAL_TABLE.keys()), default="1h",
                        help="Price interval: 1h | 4h | 1d (default: 1h)")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (default: first day of last month)")
    parser.add_argument("--end",   help="End date YYYY-MM-DD (default: last day of last month)")
    args = parser.parse_args()

    default_start, default_end = last_month_range()
    start = args.start or default_start
    end   = args.end   or default_end

    markets = MARKETS if args.market == "all" else [args.market]
    for market in markets:
        fetch_market(market, start, end, interval=args.interval)

    print("\nDone.")


if __name__ == "__main__":
    main()
