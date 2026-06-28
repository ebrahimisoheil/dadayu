from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dadayu.db import PostgresClient


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
    client: PostgresClient,
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


def check_equity_ohlcv(client: PostgresClient) -> list[CheckResult]:
    results: list[CheckResult] = []

    _check(results, "equity_daily", "Row count", client,
           "SELECT count(*) FROM stg_yahoo__equity_ohlcv_daily", fail_if_zero=True)
    _check(results, "equity_daily", "Distinct tickers", client,
           "SELECT count(DISTINCT ticker) FROM stg_yahoo__equity_ohlcv_daily", fail_if_zero=True)
    _check(results, "equity_daily", "Unsupported market rows", client,
           "SELECT count(*) FILTER (WHERE market NOT IN ('us', 'germany')) FROM stg_yahoo__equity_ohlcv_daily",
           fail_if_nonzero=True)
    _check(results, "equity_daily", "Duplicate (ticker, market, ts)", client,
           "SELECT count(*) FROM (SELECT ticker, market, ts, count(*) AS n FROM stg_yahoo__equity_ohlcv_daily GROUP BY ticker, market, ts HAVING count(*) > 1) AS dupes",
           fail_if_nonzero=True)
    _check(results, "equity_daily", "high < low violations", client,
           "SELECT count(*) FILTER (WHERE high < low) FROM stg_yahoo__equity_ohlcv_daily", fail_if_nonzero=True)
    _check(results, "equity_daily", "close outside [low, high]", client,
           """
           SELECT count(*) FILTER (WHERE
             ts < current_date
             AND (
               close < low - greatest(abs(low), 1) * 0.005
               OR close > high + greatest(abs(high), 1) * 0.005
             )
           )
           FROM stg_yahoo__equity_ohlcv_daily
           """,
           fail_if_nonzero=True)
    _check(results, "equity_daily", "Zero or negative close", client,
           "SELECT count(*) FILTER (WHERE close <= 0) FROM stg_yahoo__equity_ohlcv_daily", fail_if_nonzero=True)
    _check(results, "equity_daily", "Backfill span days", client,
           "SELECT max(ts)::date - min(ts)::date FROM stg_yahoo__equity_ohlcv_daily",
           detail="warn if materially below five years")
    results[-1].status = "WARN" if results[-1].value is None or results[-1].value < 1600 else "PASS"

    return results


def check_index_ohlcv(client: PostgresClient) -> list[CheckResult]:
    results: list[CheckResult] = []

    _check(results, "index_daily", "Row count", client,
           "SELECT count(*) FROM stg_yahoo__index_ohlcv_daily", fail_if_zero=True)
    _check(results, "index_daily", "Distinct indexes", client,
           "SELECT count(DISTINCT ticker) FROM stg_yahoo__index_ohlcv_daily", fail_if_zero=True)
    _check(results, "index_daily", "Duplicate (ticker, market, ts)", client,
           "SELECT count(*) FROM (SELECT ticker, market, ts, count(*) AS n FROM stg_yahoo__index_ohlcv_daily GROUP BY ticker, market, ts HAVING count(*) > 1) AS dupes",
           fail_if_nonzero=True)
    _check(results, "index_daily", "S&P 500 benchmark rows", client,
           "SELECT count(*) FROM int_market_indexes_daily WHERE index_id = 'sp500'",
           fail_if_zero=True)
    _check(results, "index_daily", "Backfill span days", client,
           "SELECT max(ts)::date - min(ts)::date FROM stg_yahoo__index_ohlcv_daily",
           detail="warn if materially below five years")
    results[-1].status = "WARN" if results[-1].value < 1600 else "PASS"

    return results


def check_cross_dataset(client: PostgresClient) -> list[CheckResult]:
    results: list[CheckResult] = []

    eq_stg = client.query("SELECT count(*) FROM stg_yahoo__equity_ohlcv_daily").result_rows[0][0]
    eq_int = client.query("SELECT count(*) FILTER (WHERE asset_type = 'equity') FROM int_market_assets_daily").result_rows[0][0]
    mismatch = abs(eq_stg - eq_int)
    results.append(CheckResult("cross", "stg equity daily == int market equity row count",
                               "FAIL" if mismatch != 0 else "PASS", mismatch,
                               f"stg={eq_stg:,}, int={eq_int:,}"))

    return results


def check_macro_regime(client: PostgresClient) -> list[CheckResult]:
    results: list[CheckResult] = []

    _check(results, "macro", "Macro regime row count", client,
           "SELECT count(*) FROM mart_macro_regime_daily", fail_if_zero=True)
    _check(results, "macro", "Macro regime duplicate dates", client,
           "SELECT count(*) FROM (SELECT ts, count(*) AS n FROM mart_macro_regime_daily GROUP BY ts HAVING count(*) > 1) AS dupes",
           fail_if_nonzero=True)
    _check(results, "macro", "Macro score null rows", client,
           "SELECT count(*) FILTER (WHERE composite_macro_score IS NULL OR macro_regime IS NULL) FROM mart_macro_regime_daily",
           fail_if_nonzero=True)
    _check(results, "macro", "Macro latest date age days", client,
           "SELECT current_date - max(ts)::date FROM mart_macro_regime_daily",
           detail="warn if latest macro regime is more than five calendar days old")
    results[-1].status = "WARN" if results[-1].value > 5 else "PASS"

    return results


def check_mart_sanity(client: PostgresClient) -> list[CheckResult]:
    results: list[CheckResult] = []

    _check(results, "mart", "Asset scores row count", client,
           "SELECT count(*) FROM mart_portfolio_asset_scores_daily", fail_if_zero=True)
    _check(results, "mart", "Asset scores duplicate grain", client,
           "SELECT count(*) FROM (SELECT score_date, ticker, market, count(*) AS n FROM mart_portfolio_asset_scores_daily GROUP BY score_date, ticker, market HAVING count(*) > 1) AS dupes",
           fail_if_nonzero=True)
    _check(results, "mart", "Rankable asset score null ratio", client,
           "SELECT coalesce(round((count(*) FILTER (WHERE is_rankable AND total_score IS NULL))::numeric / nullif(count(*) FILTER (WHERE is_rankable), 0), 4), 0) FROM mart_portfolio_asset_scores_daily")
    ratio = results[-1].value
    results[-1].status = "FAIL" if ratio > 0 else "PASS"
    _check(results, "mart", "Backtest trades with null prices", client,
           "SELECT count(*) FILTER (WHERE entry_price IS NULL OR exit_price IS NULL) FROM mart_backtest_trades",
           fail_if_nonzero=True)
    _check(results, "mart", "Backtest benchmark rows", client,
           "SELECT count(*) FILTER (WHERE benchmark_annualized_return_pct IS NULL) FROM mart_backtest_performance",
           fail_if_nonzero=True)

    return results


def check_product_outputs(client: PostgresClient) -> list[CheckResult]:
    results: list[CheckResult] = []

    _check(results, "product", "Universe health row count", client,
           "SELECT count(*) FROM mart_universe_health_current", fail_if_zero=True)
    _check(results, "product", "Universe health no price rows", client,
           "SELECT count(*) FROM mart_universe_health_current WHERE universe_status = 'no_price_history'",
           warn_if_nonzero=True)
    _check(results, "product", "Universe missing sector or industry rows", client,
           "SELECT count(*) FROM mart_universe_health_current WHERE universe_status = 'missing_sector_or_industry'",
           warn_if_nonzero=True)
    _check(results, "product", "Product recommendations row count", client,
           "SELECT count(*) FROM mart_product_stock_recommendations_daily", fail_if_zero=True)
    _check(results, "product", "Current top lists row count", client,
           "SELECT count(*) FROM mart_product_top_lists_current", fail_if_zero=True)
    _check(results, "product", "Current top lists complete", client,
           """
           SELECT count(*) FROM (
             SELECT list_name, count(*) AS n
             FROM mart_product_top_lists_current
             GROUP BY list_name
             HAVING (list_name = 'top_10' AND count(*) != 10)
                 OR (list_name = 'top_20' AND count(*) != 20)
                 OR (list_name = 'top_30' AND count(*) != 30)
           ) AS incomplete
           """,
           fail_if_nonzero=True)

    return results


def check_universe_membership(client: PostgresClient) -> list[CheckResult]:
    results: list[CheckResult] = []
    de = _check(results, "universe", "Active DE members", client,
               "SELECT count(*) FROM int_universe_membership_daily WHERE market = 'germany' AND valid_to IS NULL",
               detail="floor 120")
    if de < 120:
        results[-1].status = "FAIL"
    us = _check(results, "universe", "Active US members", client,
               "SELECT count(*) FROM int_universe_membership_daily WHERE market = 'us' AND valid_to IS NULL",
               detail="floor 450")
    if us < 450:
        results[-1].status = "FAIL"
    _check(results, "universe", "Overlapping spans", client,
           "SELECT count(*) FROM (SELECT a.ticker FROM int_universe_membership_daily a "
           "JOIN int_universe_membership_daily b ON a.ticker=b.ticker AND a.market=b.market "
           "AND a.valid_from < b.valid_from AND (a.valid_to IS NULL OR b.valid_from < a.valid_to)) x",
           fail_if_nonzero=True)
    return results


def run_all_checks(client: PostgresClient) -> list[CheckResult]:
    return (
        check_equity_ohlcv(client)
        + check_index_ohlcv(client)
        + check_cross_dataset(client)
        + check_macro_regime(client)
        + check_mart_sanity(client)
        + check_product_outputs(client)
        + check_universe_membership(client)
    )
