from __future__ import annotations

from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.resources import PostgresResource


@asset(
    group_name="backtests",
    deps=[
        AssetKey("mart_backtest_performance"),
        AssetKey("mart_backtest_production_candidates"),
        AssetKey("int_market_data_quality_daily"),
        AssetKey("mart_briefing_portfolio_daily"),
    ],
)
def backtest_performance_log(
    context,
    postgres: PostgresResource,
) -> MaterializeResult:
    """Log strategy performance after the weekly backtest mart refresh."""
    client = postgres.get_client()
    rows = client.query(
        """
        SELECT
            backtest_id,
            strategy_family,
            rebalance_frequency,
            portfolio_size,
            universe_scope,
            exposure_policy,
            annualized_return_pct,
            alpha_pct,
            max_drawdown_pct,
            sharpe_ratio,
            calmar_ratio
        FROM mart_backtest_performance
        ORDER BY sharpe_ratio DESC
        LIMIT 25
        """
    ).result_rows

    best_by_frequency = client.query(
        """
        SELECT
            rebalance_frequency,
            (array_agg(backtest_id ORDER BY sharpe_sort DESC))[1] AS backtest_id,
            max(sharpe_sort) AS sharpe_ratio
        FROM (
            SELECT
                rebalance_frequency,
                backtest_id,
                coalesce(sharpe_ratio, -999) AS sharpe_sort
            FROM mart_backtest_performance
        ) AS sorted
        GROUP BY rebalance_frequency
        ORDER BY rebalance_frequency
        """
    ).result_rows
    strategy_count = client.query(
        "SELECT count(*) FROM mart_backtest_performance"
    ).result_rows[0][0]
    production_candidate_count = client.query(
        """
        SELECT count(*) FILTER (WHERE is_production_candidate)
        FROM mart_backtest_production_candidates
        """
    ).result_rows[0][0]
    variant_count = client.query(
        "SELECT count(*) FROM mart_backtest_strategy_variants"
    ).result_rows[0][0]
    quality_summary = client.query(
        """
        SELECT
            count(*) FILTER (WHERE NOT has_valid_ohlc) AS invalid_ohlc_rows,
            count(*) FILTER (WHERE has_extreme_return) AS extreme_return_rows,
            count(*) FILTER (WHERE is_backtest_tradable) AS tradable_rows
        FROM int_market_data_quality_daily
        """
    ).result_rows[0]
    results = [
        {
            "backtest_id": backtest_id,
            "strategy_family": strategy_family,
            "rebalance_frequency": rebalance_frequency,
            "portfolio_size": int(portfolio_size),
            "universe_scope": universe_scope,
            "exposure_policy": exposure_policy,
            "annualized_return_pct": float(annualized_return_pct),
            "alpha_pct": float(alpha_pct),
            "max_drawdown_pct": float(max_drawdown_pct),
            "sharpe_ratio": None if sharpe_ratio is None else float(sharpe_ratio),
            "calmar_ratio": None if calmar_ratio is None else float(calmar_ratio),
        }
        for (
            backtest_id,
            strategy_family,
            rebalance_frequency,
            portfolio_size,
            universe_scope,
            exposure_policy,
            annualized_return_pct,
            alpha_pct,
            max_drawdown_pct,
            sharpe_ratio,
            calmar_ratio,
        ) in rows
    ]
    frequency_results = [
        {
            "rebalance_frequency": rebalance_frequency,
            "backtest_id": backtest_id,
            "sharpe_ratio": None if sharpe_ratio == -999 else float(sharpe_ratio),
        }
        for rebalance_frequency, backtest_id, sharpe_ratio in best_by_frequency
    ]
    context.log.info("Backtest performance: %s", results)
    context.log.info("Best backtest by frequency: %s", frequency_results)

    return MaterializeResult(
        metadata={
            "strategy_count": int(strategy_count),
            "configured_variant_count": int(variant_count),
            "production_candidate_count": int(production_candidate_count),
            "market_quality": MetadataValue.json(
                {
                    "invalid_ohlc_rows": int(quality_summary[0]),
                    "extreme_return_rows": int(quality_summary[1]),
                    "tradable_rows": int(quality_summary[2]),
                }
            ),
            "best_by_frequency": MetadataValue.json(frequency_results),
            "performance": MetadataValue.json(results),
        }
    )
