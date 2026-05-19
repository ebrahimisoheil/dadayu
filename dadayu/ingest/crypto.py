from __future__ import annotations

import csv
import time
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

UNIVERSE_CSV = Path(__file__).parent.parent.parent / "warehouse" / "seeds" / "crypto_universe.csv"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

INTERVAL_TABLE = {
    "1h": "crypto_prices_hourly",
    "4h": "crypto_prices_4h",
    "1d": "crypto_prices_daily",
}


def load_symbols() -> list[str]:
    with open(UNIVERSE_CSV, newline="") as f:
        return [row["symbol"] for row in csv.DictReader(f)]


def load_universe() -> list[dict]:
    with open(UNIVERSE_CSV, newline="") as f:
        return list(csv.DictReader(f))


def download_ohlcv(symbols: list[str], start: str, end: str, interval: str) -> pd.DataFrame:
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


def fetch_coingecko_markets(coingecko_ids: list[str]) -> list[dict]:
    ids_param = ",".join(coingecko_ids)
    url = f"{COINGECKO_BASE}/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ids_param,
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
    }

    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                print(f"  [WARN] Rate limited — waiting 60s (attempt {attempt + 1}/3)...")
                time.sleep(60)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            print(f"  [WARN] CoinGecko request failed (attempt {attempt + 1}/3): {exc}")
            time.sleep(10)

    raise RuntimeError("CoinGecko /coins/markets failed after 3 attempts")


def build_metadata(universe: list[dict], markets_data: list[dict]) -> pd.DataFrame:
    markets_by_id = {row["id"]: row for row in markets_data}
    category_by_id = {row["coingecko_id"]: row["category"] for row in universe}

    records = []
    for u in universe:
        cg_id = u["coingecko_id"]
        m = markets_by_id.get(cg_id, {})
        records.append({
            "coin_id":    cg_id,
            "symbol":     m.get("symbol", u["symbol"].replace("-USD", "").lower()),
            "name":       m.get("name", u["name"]),
            "rank":       int(m["market_cap_rank"]) if m.get("market_cap_rank") else 0,
            "market_cap": m.get("market_cap"),
            "category":   category_by_id.get(cg_id, ""),
            "chain":      "",
            "fetched_at": pd.Timestamp.now(),
        })

    df = pd.DataFrame(records)
    df["rank"] = df["rank"].astype("uint32")
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
    return df
