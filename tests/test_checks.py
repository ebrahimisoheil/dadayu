from unittest.mock import MagicMock

from dadayu.checks import (
    CheckResult,
    _check,
    check_equity_ohlcv,
    check_mart_sanity,
    check_universe_membership,
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
    # check_equity_ohlcv makes 8 SQL calls
    values = [1000, 50, 0, 0, 0, 0, 0, 1800]
    client = _mock_client(*values)
    results = check_equity_ohlcv(client)
    assert isinstance(results, list)
    assert all(isinstance(r, CheckResult) for r in results)
    assert len(results) == 8


def test_run_all_checks_returns_flat_list():
    # Provide enough mock values for all queries across all check functions
    values = [100] * 80
    client = _mock_client(*values)
    results = run_all_checks(client)
    assert isinstance(results, list)
    assert len(results) > 0
    assert all(r.status in ("PASS", "WARN", "FAIL") for r in results)


def test_universe_membership_fails_below_floor():
    # 3 queries: DE count, US count, overlap count
    client = _mock_client(10, 500, 0)
    results = check_universe_membership(client)
    de = next(r for r in results if r.name == "Active DE members")
    us = next(r for r in results if r.name == "Active US members")
    overlap = next(r for r in results if r.name == "Overlapping spans")
    assert de.status == "FAIL"
    assert us.status == "PASS"
    assert overlap.status == "PASS"


def test_universe_membership_passes_above_floor():
    client = _mock_client(150, 600, 0)
    results = check_universe_membership(client)
    assert all(r.status == "PASS" for r in results)


def test_universe_membership_fails_on_overlap():
    client = _mock_client(150, 600, 3)
    results = check_universe_membership(client)
    overlap = next(r for r in results if r.name == "Overlapping spans")
    assert overlap.status == "FAIL"
