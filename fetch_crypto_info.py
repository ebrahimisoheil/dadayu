#!/usr/bin/env python3
"""Fetch CoinGecko metadata for top-20 crypto assets → ClickHouse crypto_metadata.

Always appends. ReplacingMergeTree(fetched_at) deduplicates on read (FINAL).

Usage:
  python fetch_crypto_info.py
"""

from __future__ import annotations

import csv
import os
import time
from pathlib import Path

import clickhouse_connect
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

UNIVERSE_CSV = Path(__file__).parent / "warehouse" / "seeds" / "crypto_universe.csv"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"


def get_ch_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        database=os.environ.get("CLICKHOUSE_DB", "dadayu"),
        username=os.environ.get("CLICKHOUSE_USER", "dadayu"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


def load_universe() -> list[dict]:
    with open(UNIVERSE_CSV, newline="") as f:
        return list(csv.DictReader(f))


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


def insert_metadata(df: pd.DataFrame) -> None:
    client = get_ch_client()
    cols = ["coin_id", "symbol", "name", "rank", "market_cap", "category", "chain", "fetched_at"]
    client.insert_df("crypto_metadata", df[cols])
    print(f"  Inserted {len(df)} rows into crypto_metadata")


def main() -> None:
    print("\n=== CRYPTO METADATA — CoinGecko ===")
    universe = load_universe()
    coingecko_ids = [row["coingecko_id"] for row in universe]
    print(f"  Fetching data for {len(coingecko_ids)} coins...")

    markets_data = fetch_coingecko_markets(coingecko_ids)
    print(f"  Got {len(markets_data)} records from CoinGecko")

    df = build_metadata(universe, markets_data)
    insert_metadata(df)
    print("\nDone.")


if __name__ == "__main__":
    main()
