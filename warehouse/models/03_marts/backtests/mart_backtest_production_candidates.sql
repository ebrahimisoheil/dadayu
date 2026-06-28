{{ config(
    materialized='table',
) }}

WITH walk_forward AS (
    SELECT
        backtest_id,
        max(sharpe_ratio) FILTER (WHERE window_name = 'train') AS train_sharpe_ratio,
        max(sharpe_ratio) FILTER (WHERE window_name = 'validation') AS validation_sharpe_ratio,
        max(sharpe_ratio) FILTER (WHERE window_name = 'test') AS test_sharpe_ratio,
        max(sharpe_ratio) FILTER (WHERE window_name = 'stress') AS stress_sharpe_ratio,
        max(window_return_pct) FILTER (WHERE window_name = 'train') AS train_return_pct,
        max(window_return_pct) FILTER (WHERE window_name = 'validation') AS validation_return_pct,
        max(window_return_pct) FILTER (WHERE window_name = 'test') AS test_return_pct,
        max(window_return_pct) FILTER (WHERE window_name = 'stress') AS stress_return_pct,
        min(max_drawdown_pct) FILTER (WHERE window_name = 'train') AS train_max_drawdown_pct,
        min(max_drawdown_pct) FILTER (WHERE window_name = 'validation') AS validation_max_drawdown_pct,
        min(max_drawdown_pct) FILTER (WHERE window_name = 'test') AS test_max_drawdown_pct,
        min(max_drawdown_pct) FILTER (WHERE window_name = 'stress') AS stress_max_drawdown_pct,
        count(DISTINCT window_name) AS walk_forward_window_count
    FROM {{ ref('mart_backtest_walk_forward_performance') }}
    GROUP BY backtest_id
),

yearly AS (
    SELECT
        backtest_id,
        count(*) AS evaluated_years,
        count(*) FILTER (WHERE period_return_pct > 0) AS positive_years,
        min(period_return_pct) AS worst_year_return_pct,
        avg(period_return_pct) AS avg_year_return_pct
    FROM {{ ref('mart_backtest_performance_by_period') }}
    WHERE period_type = 'year'
      AND trading_days >= 60
    GROUP BY backtest_id
)

SELECT
    p.backtest_id AS backtest_id,
    p.strategy_family AS strategy_family,
    p.rebalance_frequency AS rebalance_frequency,
    p.portfolio_size AS portfolio_size,
    p.universe_scope AS universe_scope,
    p.exposure_policy AS exposure_policy,
    p.annualized_return_pct,
    p.benchmark_annualized_return_pct,
    p.alpha_pct,
    p.max_drawdown_pct,
    p.annualized_volatility_pct,
    p.sharpe_ratio,
    p.calmar_ratio,
    p.total_trades,
    p.rebalance_count,
    p.weighted_total_cost_pct,
    p.annualized_cost_drag_bps,
    p.profit_factor,
    p.win_rate_pct,
    c.top_contributor_ticker,
    c.top_contributor_market,
    c.top1_abs_contribution_share_pct,
    c.top5_abs_contribution_share_pct,
    c.contributing_ticker_count,
    w.train_sharpe_ratio,
    w.validation_sharpe_ratio,
    w.test_sharpe_ratio,
    w.stress_sharpe_ratio,
    w.train_return_pct,
    w.validation_return_pct,
    w.test_return_pct,
    w.stress_return_pct,
    w.train_max_drawdown_pct,
    w.validation_max_drawdown_pct,
    w.test_max_drawdown_pct,
    w.stress_max_drawdown_pct,
    y.evaluated_years,
    y.positive_years,
    y.worst_year_return_pct,
    y.avg_year_return_pct,
    p.benchmark_annualized_return_pct IS NOT NULL
        AND p.total_trades >= 100
        AND p.sharpe_ratio >= 0.75
        AND p.max_drawdown_pct >= -45
        AND coalesce(w.walk_forward_window_count, 0) >= 3
        AND coalesce(w.train_sharpe_ratio, -999) > 0
        AND coalesce(w.validation_sharpe_ratio, -999) > 0
        AND coalesce(w.test_sharpe_ratio, -999) > 0
        AND coalesce(w.stress_sharpe_ratio, -999) > 0
        AND coalesce(c.top1_abs_contribution_share_pct, 100) <= 35
        AND coalesce(c.top5_abs_contribution_share_pct, 100) <= 75
        AND coalesce(y.evaluated_years, 0) >= 3
        AND coalesce(y.positive_years, 0) >= greatest(2, floor(coalesce(y.evaluated_years, 0)::numeric / 2))
        AS is_production_candidate,
    array_to_string(array_remove(ARRAY[
        CASE WHEN p.benchmark_annualized_return_pct IS NULL THEN 'missing_benchmark' ELSE '' END,
        CASE WHEN p.total_trades < 100 THEN 'too_few_trades' ELSE '' END,
        CASE WHEN p.sharpe_ratio < 0.75 OR p.sharpe_ratio IS NULL THEN 'low_sharpe' ELSE '' END,
        CASE WHEN p.max_drawdown_pct < -45 OR p.max_drawdown_pct IS NULL THEN 'drawdown_too_deep' ELSE '' END,
        CASE WHEN coalesce(w.walk_forward_window_count, 0) < 3 THEN 'missing_walk_forward_windows' ELSE '' END,
        CASE WHEN coalesce(w.train_sharpe_ratio, -999) <= 0 THEN 'train_sharpe_non_positive' ELSE '' END,
        CASE WHEN coalesce(w.validation_sharpe_ratio, -999) <= 0 THEN 'validation_sharpe_non_positive' ELSE '' END,
        CASE WHEN coalesce(w.test_sharpe_ratio, -999) <= 0 THEN 'test_sharpe_non_positive' ELSE '' END,
        CASE WHEN coalesce(w.stress_sharpe_ratio, -999) <= 0 THEN 'stress_sharpe_non_positive' ELSE '' END,
        CASE WHEN coalesce(c.top1_abs_contribution_share_pct, 100) > 35 THEN 'top1_concentration_high' ELSE '' END,
        CASE WHEN coalesce(c.top5_abs_contribution_share_pct, 100) > 75 THEN 'top5_concentration_high' ELSE '' END,
        CASE WHEN coalesce(y.evaluated_years, 0) < 3 THEN 'too_few_years' ELSE '' END,
        CASE WHEN coalesce(y.positive_years, 0) < greatest(2, floor(coalesce(y.evaluated_years, 0)::numeric / 2)) THEN 'yearly_consistency_low' ELSE '' END
    ], ''), '|') AS rejection_reasons,
    now() AS computed_at
FROM {{ ref('mart_backtest_performance') }} AS p
LEFT JOIN walk_forward AS w
    ON p.backtest_id = w.backtest_id
LEFT JOIN yearly AS y
    ON p.backtest_id = y.backtest_id
LEFT JOIN {{ ref('mart_backtest_concentration_summary') }} AS c
    ON p.backtest_id = c.backtest_id
