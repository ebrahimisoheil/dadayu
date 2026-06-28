{{ config(
    materialized='table',
) }}

WITH enriched_trades AS (
    SELECT
        t.*,
        b.sector,
        b.industry,
        t.position_weight * coalesce(t.net_return_pct, 0) AS weighted_net_contribution_pct
    FROM {{ ref('mart_backtest_trades') }} AS t
    LEFT JOIN {{ ref('mart_briefing_portfolio_daily') }} AS b
        ON t.ticker = b.ticker
        AND t.market = b.market
        AND t.rebalance_date = b.score_date
),

grouped AS (
    SELECT
        backtest_id,
        (array_agg(strategy_family))[1] AS strategy_family,
        (array_agg(rebalance_frequency))[1] AS rebalance_frequency,
        (array_agg(portfolio_size))[1] AS portfolio_size,
        (array_agg(universe_scope))[1] AS universe_scope,
        (array_agg(exposure_policy))[1] AS exposure_policy,
        ticker,
        market,
        (array_agg(asset_type))[1] AS asset_type,
        coalesce((array_agg(sector))[1], '') AS sector,
        coalesce((array_agg(industry))[1], '') AS industry,
        count(*) AS trade_count,
        avg(position_weight) AS avg_position_weight,
        avg(net_return_pct) AS avg_net_trade_return_pct,
        sum(weighted_net_contribution_pct) AS weighted_net_contribution_pct,
        max(net_return_pct) AS best_trade_pct,
        min(net_return_pct) AS worst_trade_pct
    FROM enriched_trades
    GROUP BY backtest_id, ticker, market
)

SELECT
    *,
    abs(weighted_net_contribution_pct) AS abs_weighted_net_contribution_pct
FROM grouped
