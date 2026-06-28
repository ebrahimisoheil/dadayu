{{ config(
    materialized='table',
) }}

WITH trades_with_regime AS (
    SELECT
        t.*,
        coalesce(r.market_regime, 'unknown') AS market_regime
    FROM {{ ref('mart_backtest_trades') }} AS t
    LEFT JOIN {{ ref('mart_market_regime_daily') }} AS r
        ON t.rebalance_date = r.ts
),

segments AS (
    SELECT *, 'asset_type' AS segment_type, asset_type AS segment_value FROM trades_with_regime
    UNION ALL
    SELECT *, 'market' AS segment_type, market AS segment_value FROM trades_with_regime
    UNION ALL
    SELECT *, 'risk_bucket' AS segment_type, risk_bucket AS segment_value FROM trades_with_regime
    UNION ALL
    SELECT *, 'market_regime' AS segment_type, market_regime AS segment_value FROM trades_with_regime
    UNION ALL
    SELECT *, 'universe_scope' AS segment_type, universe_scope AS segment_value FROM trades_with_regime
    UNION ALL
    SELECT *, 'exposure_policy' AS segment_type, exposure_policy AS segment_value FROM trades_with_regime
)

SELECT
    backtest_id,
    (array_agg(strategy_family))[1] AS strategy_family,
    (array_agg(rebalance_frequency))[1] AS rebalance_frequency,
    (array_agg(portfolio_size))[1] AS portfolio_size,
    (array_agg(universe_scope))[1] AS universe_scope,
    (array_agg(exposure_policy))[1] AS exposure_policy,
    segment_type,
    segment_value,
    count(*) AS total_trades,
    avg(gross_return_pct) AS avg_gross_trade_return_pct,
    avg(net_return_pct) AS avg_net_trade_return_pct,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY net_return_pct) AS median_net_trade_return_pct,
    (count(*) FILTER (WHERE net_return_pct > 0))::double precision / nullif(count(*), 0) * 100 AS win_rate_pct,
    sum(net_return_pct) FILTER (WHERE net_return_pct > 0)
        / nullif(abs(sum(net_return_pct) FILTER (WHERE net_return_pct < 0)), 0) AS profit_factor,
    max(net_return_pct) AS best_trade_pct,
    min(net_return_pct) AS worst_trade_pct,
    avg(total_cost_pct) AS avg_total_cost_pct
FROM segments
GROUP BY backtest_id, segment_type, segment_value
