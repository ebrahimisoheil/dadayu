import pandas as pd
from unittest.mock import MagicMock
from dadayu.insert import insert_ohlcv


def _base_df() -> pd.DataFrame:
    return pd.DataFrame({
        "Datetime": pd.to_datetime(["2026-03-01 10:00:00", "2026-03-01 11:00:00"]),
        "Ticker":   ["BTC-USD", "BTC-USD"],
        "Open":     [60000.0, 61000.0],
        "High":     [61000.0, 62000.0],
        "Low":      [59000.0, 60000.0],
        "Close":    [60500.0, 61500.0],
        "Volume":   [1000, 2000],
    })


def test_insert_ohlcv_renames_columns():
    client = MagicMock()
    df = _base_df()
    insert_ohlcv(client, "crypto_prices_hourly", df, "crypto", "1h")
    inserted = client.insert_df.call_args[0][1]
    assert "ticker" in inserted.columns
    assert "datetime" in inserted.columns
    assert "close" in inserted.columns
    assert "market" in inserted.columns
    assert inserted["market"].iloc[0] == "crypto"


def test_insert_ohlcv_daily_uses_date_col():
    client = MagicMock()
    df = _base_df()
    df["Datetime"] = pd.to_datetime(["2026-03-01", "2026-03-02"])
    insert_ohlcv(client, "crypto_prices_daily", df, "crypto", "1d")
    inserted = client.insert_df.call_args[0][1]
    assert "date" in inserted.columns
    assert "datetime" not in inserted.columns


def test_insert_ohlcv_clips_negative_volume():
    client = MagicMock()
    df = _base_df()
    df["Volume"] = [-1, 500]
    insert_ohlcv(client, "prices_hourly", df, "us", "1h")
    inserted = client.insert_df.call_args[0][1]
    assert inserted["volume"].iloc[0] == 0
    assert inserted["volume"].iloc[1] == 500
