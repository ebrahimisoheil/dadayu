{{ config(
    materialized='table',
) }}

WITH ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY backtest_id
            ORDER BY abs_weighted_net_contribution_pct DESC, ticker ASC, market ASC
        ) AS contribution_rank,
        sum(abs_weighted_net_contribution_pct) OVER (
            PARTITION BY backtest_id
        ) AS total_abs_contribution_pct
    FROM {{ ref('mart_backtest_contribution_by_ticker') }}
),

summary AS (
    SELECT
        backtest_id,
        (array_agg(strategy_family))[1] AS strategy_family,
        (array_agg(rebalance_frequency))[1] AS rebalance_frequency,
        (array_agg(portfolio_size))[1] AS portfolio_size,
        (array_agg(universe_scope))[1] AS universe_scope,
        (array_agg(exposure_policy))[1] AS exposure_policy,
        max(ticker) FILTER (WHERE contribution_rank = 1) AS top_contributor_ticker,
        max(market) FILTER (WHERE contribution_rank = 1) AS top_contributor_market,
        max(weighted_net_contribution_pct) FILTER (WHERE contribution_rank = 1) AS top_contributor_pct,
        max(total_abs_contribution_pct) AS total_abs_contribution_pct,
        sum(abs_weighted_net_contribution_pct) FILTER (WHERE contribution_rank = 1) AS top1_abs_contribution_pct,
        sum(abs_weighted_net_contribution_pct) FILTER (WHERE contribution_rank <= 5) AS top5_abs_contribution_pct,
        count(*) AS contributing_ticker_count
    FROM ranked
    GROUP BY backtest_id
)

SELECT
    *,
    top1_abs_contribution_pct / nullif(total_abs_contribution_pct, 0) * 100 AS top1_abs_contribution_share_pct,
    top5_abs_contribution_pct / nullif(total_abs_contribution_pct, 0) * 100 AS top5_abs_contribution_share_pct
FROM summary
