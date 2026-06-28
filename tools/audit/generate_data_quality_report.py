from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dadayu.db import get_pg_client


def query_one(client, sql: str) -> tuple[Any, ...]:
    rows = client.query(sql).result_rows
    return rows[0] if rows else tuple()


def query_rows(client, sql: str) -> list[tuple[Any, ...]]:
    return list(client.query(sql).result_rows)


def relation_exists(client, relation_name: str) -> bool:
    row = query_one(
        client,
        f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'dadayu'
              AND table_name = '{relation_name}'
        )
        """,
    )
    return bool(row and row[0])


def markdown_table(headers: list[str], rows: list[tuple[Any, ...]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output")
    args = parser.parse_args()

    client = get_pg_client()

    raw_rows = query_rows(
        client,
        """
        SELECT 'prices_daily' AS dataset, count(*), min(date), max(date), count(DISTINCT (ticker, market))
        FROM prices_daily
        UNION ALL
        SELECT 'index_prices_daily', count(*), min(date), max(date), count(DISTINCT (ticker, market))
        FROM index_prices_daily
        UNION ALL
        SELECT 'macro_prices_daily', count(*), min(date), max(date), count(DISTINCT (ticker, market))
        FROM macro_prices_daily
        """,
    )
    metadata_rows = query_rows(
        client,
        """
        SELECT
            market,
            count(*) AS tickers,
            count(*) FILTER (WHERE nullif(sector, '') IS NOT NULL) AS with_sector,
            count(*) FILTER (WHERE nullif(industry, '') IS NOT NULL) AS with_industry,
            count(*) FILTER (WHERE market_cap IS NOT NULL) AS with_market_cap
        FROM tickers
        GROUP BY market
        ORDER BY market
        """,
    )
    universe_health_rows = query_rows(
        client,
        """
        SELECT universe_status, count(*) AS tickers
        FROM mart_universe_health_current
        GROUP BY universe_status
        ORDER BY tickers DESC, universe_status
        """,
    )
    latest_sector_rows = query_rows(
        client,
        """
        SELECT
            score_date,
            market,
            sector,
            sector_rank_in_market,
            rankable_count,
            avg_total_score,
            breadth_above_sma_200_pct,
            top_ticker
        FROM mart_sector_scores_daily
        WHERE score_date = (SELECT max(score_date) FROM mart_sector_scores_daily)
          AND sector_rank_in_market <= 5
        ORDER BY market, sector_rank_in_market
        """,
    )
    latest_industry_rows = query_rows(
        client,
        """
        SELECT
            score_date,
            market,
            sector,
            industry,
            industry_rank_in_market,
            rankable_count,
            avg_total_score,
            top_ticker
        FROM mart_industry_scores_daily
        WHERE score_date = (SELECT max(score_date) FROM mart_industry_scores_daily)
          AND industry_rank_in_market <= 5
        ORDER BY market, industry_rank_in_market
        """,
    )
    current_product_rows = query_rows(
        client,
        """
        SELECT
            list_name,
            list_rank,
            ticker,
            market,
            name,
            sector,
            industry,
            round(total_score::numeric, 2),
            risk_bucket,
            primary_signal_reason
        FROM mart_product_top_lists_current
        WHERE list_name = 'top_10'
        ORDER BY list_rank
        """,
    )
    coverage_rows = query_rows(
        client,
        """
        SELECT 'market_quality' AS dataset, count(*), min(ts), max(ts), count(DISTINCT (ticker, market))
        FROM int_market_data_quality_daily
        UNION ALL
        SELECT 'briefing_daily', count(*), min(score_date), max(score_date), count(DISTINCT (ticker, market))
        FROM mart_briefing_portfolio_daily
        UNION ALL
        SELECT 'backtest_signals', count(*), min(signal_date), max(signal_date), count(DISTINCT backtest_id)
        FROM mart_backtest_signals_daily
        UNION ALL
        SELECT 'backtest_trades', count(*), min(entry_date), max(exit_date), count(DISTINCT backtest_id)
        FROM mart_backtest_trades
        """,
    )
    product_rows = query_rows(
        client,
        """
        SELECT 'portfolio_scores_daily' AS dataset, count(*), min(score_date), max(score_date), count(DISTINCT (ticker, market))
        FROM mart_portfolio_asset_scores_daily
        UNION ALL
        SELECT 'briefing_weekly', count(*), min(period_end), max(period_end), count(DISTINCT (ticker, market))
        FROM mart_briefing_portfolio_weekly
        UNION ALL
        SELECT 'briefing_monthly', count(*), min(period_end), max(period_end), count(DISTINCT (ticker, market))
        FROM mart_briefing_portfolio_monthly
        UNION ALL
        SELECT 'briefing_quarterly', count(*), min(period_end), max(period_end), count(DISTINCT (ticker, market))
        FROM mart_briefing_portfolio_quarterly
        UNION ALL
        SELECT 'briefing_6m', count(*), min(period_end), max(period_end), count(DISTINCT (ticker, market))
        FROM mart_briefing_portfolio_6m
        """,
    )
    quality = query_one(
        client,
        """
        SELECT
            count(*) FILTER (WHERE NOT has_valid_ohlc),
            count(*) FILTER (WHERE has_extreme_return),
            count(*) FILTER (WHERE has_stale_price),
            count(*) FILTER (WHERE is_low_liquidity),
            count(*) FILTER (WHERE is_backtest_tradable)
        FROM int_market_data_quality_daily
        """,
    )
    macro_coverage = query_one(
        client,
        """
        SELECT
            count(*),
            min(ts),
            max(ts),
            round(avg(composite_macro_score)::numeric, 2),
            (array_agg(macro_regime ORDER BY ts DESC))[1],
            (array_agg(composite_macro_score ORDER BY ts DESC))[1]
        FROM mart_macro_regime_daily
        """,
    )
    macro_distribution = query_rows(
        client,
        """
        SELECT macro_regime, count(*) AS days
        FROM mart_macro_regime_daily
        GROUP BY macro_regime
        ORDER BY days DESC
        """,
    )
    latest_macro = query_rows(
        client,
        """
        SELECT
            ts,
            macro_regime,
            composite_macro_score,
            credit_score,
            rates_score,
            inflation_score,
            dollar_score,
            growth_score,
            sector_score
        FROM mart_macro_regime_daily
        ORDER BY ts DESC
        LIMIT 5
        """,
    )
    if relation_exists(client, "mart_backtest_production_candidates"):
        backtests = query_one(
            client,
            """
            SELECT
                count(*),
                count(*) FILTER (WHERE is_production_candidate),
                round(max(sharpe_ratio)::numeric, 2),
                (array_agg(backtest_id ORDER BY coalesce(sharpe_ratio, -999) DESC))[1]
            FROM mart_backtest_production_candidates
            """,
        )
        top_candidates = query_rows(
            client,
            """
            SELECT
                backtest_id,
                round(sharpe_ratio::numeric, 2),
                round(annualized_return_pct::numeric, 2),
                round(max_drawdown_pct::numeric, 2),
                round(annualized_cost_drag_bps::numeric, 2),
                round(top1_abs_contribution_share_pct::numeric, 2),
                round(top5_abs_contribution_share_pct::numeric, 2)
            FROM mart_backtest_production_candidates
            WHERE is_production_candidate
            ORDER BY sharpe_ratio DESC
            LIMIT 15
            """,
        )
    else:
        backtests = ("missing", "missing", "missing", "mart_backtest_production_candidates missing")
        top_candidates = []

    output = Path(args.output) if args.output else REPO_ROOT / f"docs/reports/{date.today()}-generated-data-quality-report.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        "\n".join(
            [
                "# Generated DADAYU Data Quality Report",
                "",
                f"**Date:** {date.today()}",
                "",
                "## Raw Warehouse Coverage",
                "",
                markdown_table(["dataset", "rows", "min_date", "max_date", "unique_entities"], raw_rows),
                "",
                "## Equity Metadata Coverage",
                "",
                markdown_table(["market", "tickers", "with_sector", "with_industry", "with_market_cap"], metadata_rows),
                "",
                "## Universe Health",
                "",
                markdown_table(["universe_status", "tickers"], universe_health_rows),
                "",
                "## Coverage",
                "",
                markdown_table(["dataset", "rows", "min_date", "max_date", "unique_entities"], coverage_rows),
                "",
                "## Product Coverage",
                "",
                markdown_table(["dataset", "rows", "min_date", "max_date", "unique_entities"], product_rows),
                "",
                "## Market Quality",
                "",
                markdown_table(
                    ["invalid_ohlc", "extreme_return", "stale_price", "low_liquidity", "tradable_rows"],
                    [quality],
                ),
                "",
                "## Sector Leaders",
                "",
                markdown_table(
                    ["score_date", "market", "sector", "rank", "rankable", "avg_score", "breadth_above_sma_200_pct", "top_ticker"],
                    latest_sector_rows,
                ),
                "",
                "## Industry Leaders",
                "",
                markdown_table(
                    ["score_date", "market", "sector", "industry", "rank", "rankable", "avg_score", "top_ticker"],
                    latest_industry_rows,
                ),
                "",
                "## Current Product Top 10",
                "",
                markdown_table(
                    ["list", "rank", "ticker", "market", "name", "sector", "industry", "score", "risk", "reason"],
                    current_product_rows,
                ),
                "",
                "## Macro Regime",
                "",
                markdown_table(
                    ["rows", "min_date", "max_date", "avg_macro_score", "latest_regime", "latest_macro_score"],
                    [macro_coverage],
                ),
                "",
                markdown_table(["macro_regime", "days"], macro_distribution),
                "",
                markdown_table(
                    [
                        "ts",
                        "macro_regime",
                        "composite_macro_score",
                        "credit_score",
                        "rates_score",
                        "inflation_score",
                        "dollar_score",
                        "growth_score",
                        "sector_score",
                    ],
                    latest_macro,
                ),
                "",
                "## Backtest Production Candidates",
                "",
                markdown_table(
                    ["strategies", "production_candidates", "best_sharpe", "best_strategy"],
                    [backtests],
                ),
                "",
                markdown_table(
                    [
                        "backtest_id",
                        "sharpe",
                        "annual_return_pct",
                        "max_drawdown_pct",
                        "annualized_cost_drag_bps",
                        "top1_contribution_share_pct",
                        "top5_contribution_share_pct",
                    ],
                    top_candidates,
                ),
                "",
            ]
        ),
        encoding="utf-8",
    )
    print(output)


if __name__ == "__main__":
    main()
