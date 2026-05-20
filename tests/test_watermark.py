from unittest.mock import MagicMock
from dadayu.watermark import get_watermark


def _mock_client(return_val):
    client = MagicMock()
    client.query.return_value.result_rows = [[return_val]]
    return client


def test_get_watermark_returns_next_day():
    import datetime
    client = _mock_client(datetime.date(2026, 5, 15))
    result = get_watermark(client, "prices_daily", "date")
    assert result == "2026-05-16"


def test_get_watermark_with_market_uses_param():
    import datetime
    client = _mock_client(datetime.date(2026, 5, 10))
    result = get_watermark(client, "prices_hourly", "datetime", market="us")
    assert result == "2026-05-11"
    call_args = client.query.call_args
    assert "{market:String}" in call_args[0][0]
    assert call_args[1]["parameters"]["market"] == "us"


def test_get_watermark_returns_none_when_table_empty():
    client = _mock_client(None)
    result = get_watermark(client, "prices_daily", "date")
    assert result is None


def test_get_watermark_returns_none_on_exception():
    client = MagicMock()
    client.query.side_effect = Exception("connection failed")
    result = get_watermark(client, "prices_daily", "date")
    assert result is None


def test_get_watermark_with_condition_id_uses_param():
    import datetime
    client = _mock_client(datetime.datetime(2026, 5, 18, 12, 0, 0))
    result = get_watermark(client, "polymarket_prices", "ts", condition_id="0xabc123")
    assert result == "2026-05-19"
    call_args = client.query.call_args
    assert "{condition_id:String}" in call_args[0][0]
    assert call_args[1]["parameters"]["condition_id"] == "0xabc123"


def test_get_watermark_condition_id_returns_none_when_empty():
    client = _mock_client(None)
    result = get_watermark(client, "polymarket_prices", "ts", condition_id="0xabc123")
    assert result is None
