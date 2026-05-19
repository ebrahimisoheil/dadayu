from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from dagster import materialize
from dagster_pipeline.resources import ClickhouseResource
from dagster_pipeline.assets.equity import equity_ohlcv, equity_ticker_info


def test_clickhouse_resource_calls_get_ch_client():
    with patch("dadayu.db.get_ch_client") as mock_get:
        mock_get.return_value = MagicMock()
        resource = ClickhouseResource()
        client = resource.get_client()
    mock_get.assert_called_once()
    assert client is mock_get.return_value


def test_equity_ohlcv_skips_empty_download():
    with patch("dadayu.db.get_ch_client") as mock_gc, \
         patch("dagster_pipeline.assets.equity.get_tickers", return_value=["AAPL"]), \
         patch("dagster_pipeline.assets.equity.get_watermark", return_value="2026-05-01"), \
         patch("dagster_pipeline.assets.equity.download_ohlcv", return_value=pd.DataFrame()):
        mock_gc.return_value = MagicMock()
        result = materialize([equity_ohlcv], resources={"clickhouse": ClickhouseResource()})
    assert result.success


def test_equity_ticker_info_skips_market_with_no_tickers():
    mock_client = MagicMock()
    mock_client.query.return_value = MagicMock(result_rows=[])
    with patch("dadayu.db.get_ch_client", return_value=mock_client), \
         patch("dagster_pipeline.assets.equity.fetch_ticker_metadata") as mock_meta:
        result = materialize(
            [equity_ticker_info],
            resources={"clickhouse": ClickhouseResource()},
        )
    assert result.success
    mock_meta.assert_not_called()
