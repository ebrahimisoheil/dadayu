from __future__ import annotations

import pandas as pd
from dagster import AssetKey, asset

from dadayu.ingest.equity import (
    INTERVAL_TABLE,
    MARKETS,
    download_ohlcv,
    fetch_ticker_metadata,
    get_index_membership,
    get_tickers,
)
from dadayu.insert import insert_ohlcv
from dadayu.watermark import get_watermark
from dagster_pipeline.resources import PostgresResource


def _five_year_backfill_start() -> str:
    return (pd.Timestamp.today() - pd.DateOffset(years=5)).strftime("%Y-%m-%d")


def _existing_price_tickers(client, table: str, market: str) -> set[str]:
    result = client.query(
        f"SELECT DISTINCT ticker FROM {table} WHERE market = %(market)s",
        parameters={"market": market},
    )
    return {row[0] for row in result.result_rows}


@asset(group_name="ingestion")
def equity_ohlcv(postgres: PostgresResource) -> None:
    client = postgres.get_client()
    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    for market in MARKETS:
        tickers = get_tickers(market)
        if not tickers:
            continue

        for interval, table in INTERVAL_TABLE.items():
            start = get_watermark(client, table, "date", market=market) or _five_year_backfill_start()
            if pd.Timestamp(start) > pd.Timestamp(today):
                print(f"  {table} [{market}] is already current through {today}")
            else:
                prices = download_ohlcv(tickers, start, today, interval)
                if not prices.empty:
                    insert_ohlcv(client, table, prices, market, interval)

            missing = sorted(set(tickers) - _existing_price_tickers(client, table, market))
            if missing:
                print(f"  [WARN] Missing {len(missing)} {market} tickers after bulk load; retrying full backfill...")
                prices = download_ohlcv(missing, _five_year_backfill_start(), today, interval)
                if not prices.empty:
                    insert_ohlcv(client, table, prices, market, interval)


@asset(group_name="ingestion", deps=[AssetKey("equity_ticker_info")])
def equity_index_membership(postgres: PostgresResource) -> None:
    client = postgres.get_client()
    rows = []
    now = pd.Timestamp.now()
    for market in MARKETS:
        for ticker, index_name in get_index_membership(market):
            rows.append({"ticker": ticker, "market": market,
                         "index_name": index_name, "observed_at": now})
    if not rows:
        return
    df = pd.DataFrame(rows)
    client.execute(
        "CREATE TABLE IF NOT EXISTS index_membership_observed ("
        "ticker text, market text, index_name text, observed_at timestamp)"
    )
    client.execute("TRUNCATE index_membership_observed")
    client.insert_df("index_membership_observed", df)


@asset(group_name="ingestion", deps=[AssetKey("equity_ohlcv")])
def equity_ticker_info(postgres: PostgresResource) -> None:
    client = postgres.get_client()
    for market in MARKETS:
        tickers = get_tickers(market)
        if not tickers:
            continue
        df = fetch_ticker_metadata(tickers, market)
        df["fetched_at"] = pd.Timestamp.now()
        for col in ["name", "sector", "industry", "currency", "country"]:
            df[col] = df[col].fillna("").astype(str)
        for col in ["market_cap", "pe_ratio"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        cols = ["ticker", "market", "name", "sector", "industry",
                "currency", "country", "market_cap", "pe_ratio", "fetched_at"]
        client.upsert_df(
            "tickers",
            df[cols],
            conflict_cols=["ticker", "market"],
            update_cols=["name", "sector", "industry", "currency", "country", "market_cap", "pe_ratio", "fetched_at"],
        )
