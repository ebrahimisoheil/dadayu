from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from dagster import materialize
from dagster_pipeline.resources import ClickhouseResource
from dagster_pipeline.assets.equity import equity_ohlcv, equity_ticker_info
from dagster_pipeline.assets.crypto import crypto_ohlcv, crypto_info


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


def test_crypto_ohlcv_skips_empty_download():
    with patch("dadayu.db.get_ch_client") as mock_gc, \
         patch("dagster_pipeline.assets.crypto.load_symbols", return_value=["BTC-USD"]), \
         patch("dagster_pipeline.assets.crypto.get_watermark", return_value="2026-05-01"), \
         patch("dagster_pipeline.assets.crypto.download_ohlcv", return_value=pd.DataFrame()):
        mock_gc.return_value = MagicMock()
        result = materialize([crypto_ohlcv], resources={"clickhouse": ClickhouseResource()})
    assert result.success


def test_crypto_info_calls_insert():
    sample_df = pd.DataFrame([{
        "coin_id": "bitcoin", "symbol": "btc", "name": "Bitcoin",
        "rank": 1, "market_cap": 1e12, "category": "Layer 1",
        "chain": "", "fetched_at": pd.Timestamp.now(),
    }])
    mock_client = MagicMock()
    with patch("dadayu.db.get_ch_client", return_value=mock_client), \
         patch("dagster_pipeline.assets.crypto.load_universe", return_value=[{"coingecko_id": "bitcoin"}]), \
         patch("dagster_pipeline.assets.crypto.fetch_coingecko_markets", return_value=[]), \
         patch("dagster_pipeline.assets.crypto.build_metadata", return_value=sample_df):
        result = materialize([crypto_info], resources={"clickhouse": ClickhouseResource()})
    assert result.success
    mock_client.insert_df.assert_called_once()
    args = mock_client.insert_df.call_args
    assert args[0][0] == "crypto_metadata"


def test_dbt_asset_groups_load():
    from dagster_pipeline.assets.dbt import (
        data_quality,
        dbt_mart_assets,
        dbt_seed_assets,
        dbt_snapshot_assets,
        dbt_staging_assets,
    )
    for group in [dbt_seed_assets, dbt_staging_assets, dbt_snapshot_assets, dbt_mart_assets]:
        assert group is not None
    assert callable(data_quality)


def test_data_quality_asset_raises_failure_on_fail_results():
    import pytest
    from unittest.mock import MagicMock, patch
    from dagster import Failure
    from dagster_pipeline.assets.dbt.quality import data_quality
    from dagster_pipeline.resources import ClickhouseResource
    from dadayu.checks import CheckResult

    fail_results = [CheckResult("sec", "bad check", "FAIL", 5, "something wrong")]

    with patch("dadayu.db.get_ch_client") as mock_gc, \
         patch("dagster_pipeline.assets.dbt.quality.run_all_checks", return_value=fail_results):
        mock_gc.return_value = MagicMock()
        with pytest.raises(Failure):
            data_quality(clickhouse=ClickhouseResource())


def test_data_quality_asset_succeeds_on_pass_results():
    from unittest.mock import MagicMock, patch
    from dagster_pipeline.assets.dbt.quality import data_quality
    from dagster_pipeline.resources import ClickhouseResource
    from dadayu.checks import CheckResult

    pass_results = [CheckResult("sec", "good check", "PASS", 100, "")]

    with patch("dadayu.db.get_ch_client") as mock_gc, \
         patch("dagster_pipeline.assets.dbt.quality.run_all_checks", return_value=pass_results):
        mock_gc.return_value = MagicMock()
        result = data_quality(clickhouse=ClickhouseResource())
    assert result is not None
