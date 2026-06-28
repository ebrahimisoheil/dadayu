from __future__ import annotations

import pandas as pd

from dadayu.db import PostgresClient


def insert_ohlcv(
    client: PostgresClient,
    table: str,
    df: pd.DataFrame,
    market: str,
    interval: str,
) -> None:
    is_daily = interval == "1d"
    data = df.copy()
    data["market"] = market

    if is_daily:
        data["Datetime"] = pd.to_datetime(data["Datetime"]).dt.date
        data = data.rename(columns={
            "Ticker": "ticker", "Datetime": "date",
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        cols = ["ticker", "market", "date", "open", "high", "low", "close", "volume"]
        data["_ym"] = pd.to_datetime(data["date"]).dt.to_period("M")
    else:
        data["Datetime"] = pd.to_datetime(data["Datetime"]).dt.tz_localize(None)
        data = data.rename(columns={
            "Ticker": "ticker", "Datetime": "datetime",
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        cols = ["ticker", "market", "datetime", "open", "high", "low", "close", "volume"]
        data["_ym"] = data["datetime"].dt.to_period("M")

    data = data[[c for c in cols if c in data.columns] + ["_ym"]].dropna(subset=["close"])
    data["volume"] = data["volume"].fillna(0).clip(lower=0).astype(int)

    total = 0
    for _, chunk in data.groupby("_ym"):
        client.upsert_df(
            table,
            chunk.drop(columns=["_ym"]),
            conflict_cols=["ticker", "market", "date"],
            update_cols=["open", "high", "low", "close", "volume", "ingested_at"],
        )
        total += len(chunk)
    print(f"  Inserted {total:,} rows into {table} [{market}]")
