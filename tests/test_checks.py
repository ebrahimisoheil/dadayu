from unittest.mock import MagicMock

from dadayu.checks import (
    CheckResult,
    _check,
    check_equity_ohlcv,
    check_mart_sanity,
    run_all_checks,
)


def _mock_client(*return_values):
    client = MagicMock()
    client.query.side_effect = [
        MagicMock(result_rows=[[v]]) for v in return_values
    ]
    return client


def test_check_result_pass():
    results = []
    client = _mock_client(42)
    _check(results, "sec", "name", client, "SELECT 42", fail_if_zero=True)
    assert results[0].status == "PASS"
    assert results[0].value == 42


def test_check_result_fail_if_zero():
    results = []
    client = _mock_client(0)
    _check(results, "sec", "name", client, "SELECT 0", fail_if_zero=True)
    assert results[0].status == "FAIL"


def test_check_result_warn_if_nonzero():
    results = []
    client = _mock_client(5)
    _check(results, "sec", "name", client, "SELECT 5", warn_if_nonzero=True)
    assert results[0].status == "WARN"


def test_check_equity_ohlcv_returns_list():
    # check_equity_ohlcv makes 11 SQL calls
    values = [1000, 50, 2, 0, 0, 0, 0, 0, 10, 0, 0]
    client = _mock_client(*values)
    results = check_equity_ohlcv(client)
    assert isinstance(results, list)
    assert all(isinstance(r, CheckResult) for r in results)
    assert len(results) == 11


def test_run_all_checks_returns_flat_list():
    # Provide enough mock values for all queries across all 5 check functions
    values = [100] * 60
    client = _mock_client(*values)
    results = run_all_checks(client)
    assert isinstance(results, list)
    assert len(results) > 0
    assert all(r.status in ("PASS", "WARN", "FAIL") for r in results)
