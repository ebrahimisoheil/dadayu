import pandas as pd
from unittest.mock import MagicMock
from dadayu.insert import insert_ohlcv


def _base_df() -> pd.DataFrame:
    return pd.DataFrame({
        "Datetime": pd.to_datetime(["2026-03-01 10:00:00", "2026-03-01 11:00:00"]),
        "Ticker":   ["AAPL", "AAPL"],
        "Open":     [190.0, 191.0],
        "High":     [192.0, 193.0],
        "Low":      [188.0, 189.0],
        "Close":    [191.0, 192.0],
        "Volume":   [1000, 2000],
    })


def test_insert_ohlcv_renames_columns():
    client = MagicMock()
    df = _base_df()
    insert_ohlcv(client, "prices_daily", df, "us", "1d")
    inserted = client.upsert_df.call_args[0][1]
    assert "ticker" in inserted.columns
    assert "date" in inserted.columns
    assert "close" in inserted.columns
    assert "market" in inserted.columns
    assert inserted["market"].iloc[0] == "us"
    assert client.upsert_df.call_args.kwargs["conflict_cols"] == ["ticker", "market", "date"]


def test_insert_ohlcv_daily_uses_date_col():
    client = MagicMock()
    df = _base_df()
    df["Datetime"] = pd.to_datetime(["2026-03-01", "2026-03-02"])
    insert_ohlcv(client, "prices_daily", df, "us", "1d")
    inserted = client.upsert_df.call_args[0][1]
    assert "date" in inserted.columns
    assert "datetime" not in inserted.columns


def test_insert_ohlcv_clips_negative_volume():
    client = MagicMock()
    df = _base_df()
    df["Volume"] = [-1, 500]
    insert_ohlcv(client, "prices_daily", df, "us", "1d")
    inserted = client.upsert_df.call_args[0][1]
    assert inserted["volume"].iloc[0] == 0
    assert inserted["volume"].iloc[1] == 500
