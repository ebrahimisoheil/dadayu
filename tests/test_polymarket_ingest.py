from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _mock_gamma_market(condition_id="0xabc123", question="Will BTC exceed $100k?"):
    return {
        "conditionId": condition_id,
        "question": question,
        "category": "Crypto",
        "volume": "75000.50",
        "liquidity": "20000.00",
        "active": True,
        "closed": False,
        "endDate": "2025-12-31T00:00:00Z",
        "outcome": None,
        "tokens": [
            {"outcome": "Yes", "token_id": "token_yes_abc"},
            {"outcome": "No", "token_id": "token_no_abc"},
        ],
    }


def _mock_clob_response(history=None):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"history": history or []}
    return mock_resp


def test_discover_markets_parses_condition_id():
    with patch("dadayu.ingest.polymarket.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [_mock_gamma_market()]
        from dadayu.ingest.polymarket import discover_markets
        df = discover_markets(min_volume_usd=50_000)
    assert len(df) == 1
    assert df.iloc[0]["condition_id"] == "0xabc123"
    assert df.iloc[0]["yes_token_id"] == "token_yes_abc"
    assert df.iloc[0]["volume_usd"] == pytest.approx(75000.50)


def test_discover_markets_auto_parses_btc_ticker():
    with patch("dadayu.ingest.polymarket.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [_mock_gamma_market(question="Will BTC exceed $100k?")]
        from dadayu.ingest.polymarket import discover_markets
        df = discover_markets(min_volume_usd=50_000)
    assert df.iloc[0]["linked_asset"] == "BTC-USD"
    assert df.iloc[0]["asset_type"] == "crypto"


def test_discover_markets_returns_none_for_unknown_asset():
    with patch("dadayu.ingest.polymarket.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [
            _mock_gamma_market(question="Will the US avoid a recession?")
        ]
        from dadayu.ingest.polymarket import discover_markets
        df = discover_markets(min_volume_usd=50_000)
    assert df.iloc[0]["linked_asset"] is None
    assert df.iloc[0]["asset_type"] is None


def test_fetch_price_history_parses_clob_response():
    history = [
        {"t": 1700000000, "p": "0.65", "v": "1234.5"},
        {"t": 1700003600, "p": "0.70", "v": "500.0"},
    ]
    with patch("dadayu.ingest.polymarket.requests.get", return_value=_mock_clob_response(history)):
        from dadayu.ingest.polymarket import fetch_price_history
        df = fetch_price_history("token_yes_abc", 1700000000, 1700010000)
    assert len(df) == 2
    assert list(df.columns) == ["ts", "probability", "volume_usd"]
    assert df.iloc[0]["probability"] == pytest.approx(0.65)
    assert df.iloc[0]["volume_usd"] == pytest.approx(1234.5)


def test_fetch_price_history_returns_empty_on_no_history():
    with patch("dadayu.ingest.polymarket.requests.get", return_value=_mock_clob_response([])):
        from dadayu.ingest.polymarket import fetch_price_history
        df = fetch_price_history("token_yes_abc", 1700000000, 1700010000)
    assert df.empty
    assert list(df.columns) == ["ts", "probability", "volume_usd"]


def test_clob_retries_on_429():
    mock_429 = MagicMock(status_code=429)
    history = [{"t": 1700000000, "p": "0.60", "v": "100.0"}]
    mock_ok = _mock_clob_response(history)
    with patch("dadayu.ingest.polymarket.requests.get", side_effect=[mock_429, mock_ok]):
        with patch("dadayu.ingest.polymarket.time.sleep") as mock_sleep:
            from dadayu.ingest.polymarket import fetch_price_history
            df = fetch_price_history("token_yes_abc", 1700000000, 1700010000)
    mock_sleep.assert_called_once_with(1)  # 4^0 = 1 on first retry
    assert len(df) == 1


def test_discover_markets_no_false_positive_for_substring():
    with patch("dadayu.ingest.polymarket.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [
            _mock_gamma_market(question="Will ethanol production increase?")
        ]
        from dadayu.ingest.polymarket import discover_markets
        df = discover_markets(min_volume_usd=50_000)
    assert df.iloc[0]["linked_asset"] is None  # "ETH" should NOT match "ETHanol"


def test_fetch_daily_price_history_uses_fidelity_1440():
    history = [{"t": 1700000000, "p": "0.55", "v": "5000.0"}]
    with patch("dadayu.ingest.polymarket.requests.get", return_value=_mock_clob_response(history)) as mock_get:
        from dadayu.ingest.polymarket import fetch_daily_price_history
        fetch_daily_price_history("token_yes_abc", 1700000000, 1700100000)
    call_params = mock_get.call_args[1]["params"]
    assert call_params["fidelity"] == 1440
