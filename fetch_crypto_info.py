#!/usr/bin/env python3
"""Fetch CoinGecko metadata for top-20 crypto → ClickHouse crypto_metadata.

Usage:
  python fetch_crypto_info.py
"""

from __future__ import annotations

import pandas as pd

from dadayu.db import get_ch_client
from dadayu.ingest.crypto import build_metadata, fetch_coingecko_markets, load_universe


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
