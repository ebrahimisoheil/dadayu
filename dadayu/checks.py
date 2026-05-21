from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import clickhouse_connect


@dataclass
class CheckResult:
    section: str
    name: str
    status: str          # PASS | WARN | FAIL
    value: Any = None
    detail: str = ""


def _check(
    results: list[CheckResult],
    section: str,
    name: str,
    client: clickhouse_connect.driver.Client,
    sql: str,
    *,
    fail_if_nonzero: bool = False,
    warn_if_nonzero: bool = False,
    fail_if_zero: bool = False,
    warn_if_zero: bool = False,
    detail: str = "",
) -> Any:
    val = client.query(sql).result_rows[0][0]
    status = "PASS"
    if fail_if_nonzero and val != 0:
        status = "FAIL"
    elif warn_if_nonzero and val != 0:
        status = "WARN"
    elif fail_if_zero and val == 0:
        status = "FAIL"
    elif warn_if_zero and val == 0:
        status = "WARN"
    results.append(CheckResult(section, name, status, val, detail))
    return val


def check_equity_ohlcv(client: clickhouse_connect.driver.Client) -> list[CheckResult]:
    results: list[CheckResult] = []

    _check(results, "equity_1h", "Row count", client,
           "SELECT count() FROM stg_yahoo__ohlcv_1h", fail_if_zero=True)
    _check(results, "equity_1h", "Distinct tickers", client,
           "SELECT countDistinct(ticker) FROM stg_yahoo__ohlcv_1h", fail_if_zero=True)
    _check(results, "equity_1h", "Freshness — hours since last bar", client,
           "SELECT dateDiff('hour', max(ts), now()) FROM stg_yahoo__ohlcv_1h",
           detail="warn if > 26h")
    hours_old = results[-1].value
    results[-1].status = "WARN" if hours_old > 26 else "PASS"
    _check(results, "equity_1h", "Duplicate (ticker, market, ts)", client,
           "SELECT count() FROM (SELECT ticker, market, ts, count() AS n FROM stg_yahoo__ohlcv_1h GROUP BY ticker, market, ts HAVING n > 1)",
           fail_if_nonzero=True)
    _check(results, "equity_1h", "high < low violations", client,
           "SELECT countIf(high < low) FROM stg_yahoo__ohlcv_1h", fail_if_nonzero=True)
    _check(results, "equity_1h", "close outside [low, high]", client,
           "SELECT countIf(close < low OR close > high) FROM stg_yahoo__ohlcv_1h", fail_if_nonzero=True)
    _check(results, "equity_1h", "open outside [low, high]", client,
           "SELECT countIf(open < low OR open > high) FROM stg_yahoo__ohlcv_1h", fail_if_nonzero=True)
    _check(results, "equity_1h", "Zero or negative close", client,
           "SELECT countIf(close <= 0) FROM stg_yahoo__ohlcv_1h", fail_if_nonzero=True)
    _check(results, "equity_1h", "Zero volume bars", client,
           "SELECT countIf(volume = 0) FROM stg_yahoo__ohlcv_1h",
           warn_if_nonzero=True, detail="may be legit on holidays/halts")
    _check(results, "equity_1h", "Null prices", client,
           "SELECT countIf(isNull(open) OR isNull(high) OR isNull(low) OR isNull(close)) FROM stg_yahoo__ohlcv_1h",
           fail_if_nonzero=True)
    _check(results, "equity_1h", "Bars with >50% single-hour return", client,
           """
           SELECT count() FROM (
             SELECT ticker, market, ts,
               (close - lagInFrame(close, 1, close) OVER w) / nullIf(lagInFrame(close, 1, close) OVER w, 0) AS ret
             FROM stg_yahoo__ohlcv_1h
             WINDOW w AS (PARTITION BY ticker, market ORDER BY ts)
           ) WHERE abs(ret) > 0.5
           """,
           warn_if_nonzero=True, detail=">50% single-bar move")

    return results


def check_crypto_ohlcv(client: clickhouse_connect.driver.Client) -> list[CheckResult]:
    results: list[CheckResult] = []

    _check(results, "crypto_1h", "Row count", client,
           "SELECT count() FROM stg_yahoo__crypto_ohlcv_1h", fail_if_zero=True)
    _check(results, "crypto_1h", "Distinct tickers", client,
           "SELECT countDistinct(ticker) FROM stg_yahoo__crypto_ohlcv_1h", fail_if_zero=True)
    _check(results, "crypto_1h", "Freshness — hours since last bar", client,
           "SELECT dateDiff('hour', max(ts), now()) FROM stg_yahoo__crypto_ohlcv_1h")
    hours_old = results[-1].value
    results[-1].status = "WARN" if hours_old > 5 else "PASS"
    results[-1].detail = "warn if > 5h (crypto never closes)"
    _check(results, "crypto_1h", "Duplicate (ticker, market, ts)", client,
           "SELECT count() FROM (SELECT ticker, market, ts, count() AS n FROM stg_yahoo__crypto_ohlcv_1h GROUP BY ticker, market, ts HAVING n > 1)",
           fail_if_nonzero=True)
    _check(results, "crypto_1h", "high < low violations", client,
           "SELECT countIf(high < low) FROM stg_yahoo__crypto_ohlcv_1h", fail_if_nonzero=True)
    _check(results, "crypto_1h", "close outside [low, high]", client,
           "SELECT countIf(close < low OR close > high) FROM stg_yahoo__crypto_ohlcv_1h", fail_if_nonzero=True)
    _check(results, "crypto_1h", "Zero or negative close", client,
           "SELECT countIf(close <= 0) FROM stg_yahoo__crypto_ohlcv_1h", fail_if_nonzero=True)
    _check(results, "crypto_1h", "Bars with >30% single-hour return", client,
           """
           SELECT count() FROM (
             SELECT ticker, market, ts,
               (close - lagInFrame(close, 1, close) OVER w) / nullIf(lagInFrame(close, 1, close) OVER w, 0) AS ret
             FROM stg_yahoo__crypto_ohlcv_1h
             WINDOW w AS (PARTITION BY ticker, market ORDER BY ts)
           ) WHERE abs(ret) > 0.3
           """,
           warn_if_nonzero=True, detail=">30% single-bar move")

    return results


def check_polymarket(client: clickhouse_connect.driver.Client) -> list[CheckResult]:
    results: list[CheckResult] = []

    _check(results, "polymarket", "Total markets", client,
           "SELECT count() FROM polymarket_markets FINAL", fail_if_zero=True)
    _check(results, "polymarket", "Active markets (not closed)", client,
           "SELECT countIf(closed = false) FROM polymarket_markets FINAL", warn_if_zero=True)
    _check(results, "polymarket", "Markets missing yes_token_id", client,
           "SELECT countIf(yes_token_id = '') FROM polymarket_markets FINAL WHERE closed = false",
           warn_if_nonzero=True, detail="these markets won't get price data")
    _check(results, "polymarket", "Freshness — hours since last market fetch", client,
           "SELECT dateDiff('hour', max(fetched_at), now()) FROM polymarket_markets")
    hours_old = results[-1].value
    results[-1].status = "WARN" if hours_old > 26 else "PASS"
    _check(results, "polymarket", "Price rows total", client,
           "SELECT count() FROM polymarket_prices", fail_if_zero=True)
    _check(results, "polymarket", "Markets with price data", client,
           "SELECT countDistinct(condition_id) FROM polymarket_prices", fail_if_zero=True)
    _check(results, "polymarket", "Active markets with NO price data", client,
           """
           SELECT count() FROM (
             SELECT condition_id FROM polymarket_markets FINAL WHERE closed = false AND yes_token_id != ''
           ) WHERE condition_id NOT IN (SELECT DISTINCT condition_id FROM polymarket_prices)
           """,
           warn_if_nonzero=True, detail="should be 0 after first run")
    _check(results, "polymarket", "Probability outside [0, 1]", client,
           "SELECT countIf(probability < 0 OR probability > 1) FROM polymarket_prices",
           fail_if_nonzero=True)
    _check(results, "polymarket", "Duplicate (condition_id, ts)", client,
           "SELECT count() FROM (SELECT condition_id, ts, count() AS n FROM polymarket_prices GROUP BY condition_id, ts HAVING n > 1)",
           warn_if_nonzero=True)
    _check(results, "polymarket", "Freshness — hours since last price", client,
           "SELECT dateDiff('hour', max(ts), now()) FROM polymarket_prices")
    hours_old = results[-1].value
    results[-1].status = "WARN" if hours_old > 5 else "PASS"
    results[-1].detail = "warn if > 5h"
    _check(results, "polymarket", "Extreme prob jump >0.5 in single hour", client,
           """
           SELECT countIf(abs(prob_change) > 0.5)
           FROM fct_polymarket_signals
           WHERE days_to_resolution > 2 OR days_to_resolution IS NULL
           """,
           warn_if_nonzero=True, detail="excludes near-expiry rows")

    return results


def check_cross_dataset(client: clickhouse_connect.driver.Client) -> list[CheckResult]:
    results: list[CheckResult] = []

    eq_stg = client.query("SELECT count() FROM stg_yahoo__ohlcv_1h").result_rows[0][0]
    eq_int = client.query("SELECT count() FROM int_equity_ohlcv_1h").result_rows[0][0]
    mismatch = abs(eq_stg - eq_int)
    results.append(CheckResult("cross", "stg_yahoo__ohlcv_1h == int_equity_ohlcv_1h row count",
                               "FAIL" if mismatch != 0 else "PASS", mismatch,
                               f"stg={eq_stg:,}, int={eq_int:,}"))

    eq_int = client.query("SELECT count() FROM int_equity_ohlcv_1h").result_rows[0][0]
    eq_fct = client.query("SELECT count() FROM fct_ohlcv_1h").result_rows[0][0]
    mismatch = abs(eq_int - eq_fct)
    results.append(CheckResult("cross", "int_equity_ohlcv_1h == fct_ohlcv_1h row count",
                               "FAIL" if mismatch != 0 else "PASS", mismatch,
                               f"int={eq_int:,}, fct={eq_fct:,}"))

    cr_stg = client.query("SELECT count() FROM stg_yahoo__crypto_ohlcv_1h").result_rows[0][0]
    cr_int = client.query("SELECT count() FROM int_crypto_ohlcv_1h").result_rows[0][0]
    mismatch = abs(cr_stg - cr_int)
    results.append(CheckResult("cross", "stg_crypto_ohlcv_1h == int_crypto_ohlcv_1h row count",
                               "FAIL" if mismatch != 0 else "PASS", mismatch,
                               f"stg={cr_stg:,}, int={cr_int:,}"))

    pm_stg = client.query("SELECT countDistinct(condition_id) FROM stg_polymarket__prices").result_rows[0][0]
    pm_int = client.query("SELECT countDistinct(condition_id) FROM int_polymarket_prices_1h").result_rows[0][0]
    results.append(CheckResult("cross", "Polymarket: stg markets == int_1h markets",
                               "FAIL" if pm_stg != pm_int else "PASS", None,
                               f"stg={pm_stg}, int={pm_int}"))

    _check(results, "cross", "Equity tickers not in dim_equity_symbol", client,
           """
           SELECT countDistinct(ticker) FROM stg_yahoo__ohlcv_1h
           WHERE ticker NOT IN (SELECT ticker FROM dim_equity_symbol)
           """,
           warn_if_nonzero=True, detail="dim may lag by 1 run")
    _check(results, "cross", "Crypto tickers not in dim_crypto_symbol", client,
           """
           SELECT countDistinct(ticker) FROM stg_yahoo__crypto_ohlcv_1h
           WHERE ticker NOT IN (SELECT yf_symbol FROM dim_crypto_symbol)
           """,
           warn_if_nonzero=True)

    return results


def check_mart_sanity(client: clickhouse_connect.driver.Client) -> list[CheckResult]:
    results: list[CheckResult] = []

    _check(results, "mart", "fct_ohlcv_1h: null return_pct ratio", client,
           "SELECT round(countIf(isNull(return_pct)) / count(), 4) FROM fct_ohlcv_1h",
           detail="null only on first bar per ticker; >0.01 is suspicious")
    ratio = results[-1].value
    results[-1].status = "WARN" if ratio > 0.01 else "PASS"

    _check(results, "mart", "fct_indicators_1h: null RSI rows", client,
           "SELECT countIf(isNull(rsi_14)) FROM fct_indicators_1h",
           warn_if_nonzero=True, detail="RSI null on first 14 bars per ticker only")
    _check(results, "mart", "fct_indicators_1h: RSI outside [0, 100]", client,
           "SELECT countIf(rsi_14 < 0 OR rsi_14 > 100) FROM fct_indicators_1h WHERE rsi_14 IS NOT NULL",
           fail_if_nonzero=True)
    _check(results, "mart", "fct_polymarket_signals: null prob_change ratio", client,
           "SELECT round(countIf(isNull(prob_change)) / count(), 4) FROM fct_polymarket_signals",
           detail="null only on first bar per market")
    ratio = results[-1].value
    results[-1].status = "WARN" if ratio > 0.02 else "PASS"
    _check(results, "mart", "fct_polymarket_signals: log_odds finite", client,
           "SELECT countIf(isInfinite(log_odds) OR isNaN(log_odds)) FROM fct_polymarket_signals",
           fail_if_nonzero=True)

    return results


def run_all_checks(client: clickhouse_connect.driver.Client) -> list[CheckResult]:
    return (
        check_equity_ohlcv(client)
        + check_crypto_ohlcv(client)
        + check_polymarket(client)
        + check_cross_dataset(client)
        + check_mart_sanity(client)
    )
