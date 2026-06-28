{{ config(
    materialized='table',
) }}

WITH trades_summary AS (
    SELECT
        backtest_id,
        (array_agg(strategy_family))[1] AS strategy_family,
        (array_agg(rebalance_frequency))[1] AS rebalance_frequency,
        (array_agg(portfolio_size))[1] AS portfolio_size,
        (array_agg(universe_scope))[1] AS universe_scope,
        (array_agg(exposure_policy))[1] AS exposure_policy,
        count(*) AS total_trades,
        count(DISTINCT rebalance_date) AS rebalance_count,
        avg(gross_return_pct) AS avg_gross_trade_return_pct,
        avg(net_return_pct) AS avg_net_trade_return_pct,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY net_return_pct) AS median_trade_return_pct,
        percentile_cont(0.05) WITHIN GROUP (ORDER BY net_return_pct) AS p05_trade_return_pct,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY net_return_pct) AS p95_trade_return_pct,
        stddev_pop(net_return_pct) AS std_trade_return_pct,
        (count(*) FILTER (WHERE net_return_pct > 0))::double precision / nullif(count(*), 0) AS win_rate,
        sum(net_return_pct) FILTER (WHERE net_return_pct > 0)
            / nullif(abs(sum(net_return_pct) FILTER (WHERE net_return_pct < 0)), 0) AS profit_factor,
        max(net_return_pct) AS best_trade_pct,
        min(net_return_pct) AS worst_trade_pct,
        avg(holding_days) AS avg_holding_days,
        sum(total_cost_pct * position_weight) AS weighted_total_cost_pct,
        avg(actual_position_count) AS avg_actual_position_count
    FROM {{ ref('mart_backtest_trades') }}
    GROUP BY backtest_id
),

equity_summary_base AS (
    SELECT
        backtest_id,
        min(equity_date) AS start_date,
        max(equity_date) AS end_date,
        (array_agg(portfolio_value ORDER BY equity_date DESC))[1] AS final_value,
        min(drawdown_pct) AS max_drawdown_pct,
        avg(portfolio_return_pct) AS avg_daily_return_pct,
        stddev_pop(portfolio_return_pct) AS std_daily_return_pct,
        (count(*) FILTER (WHERE portfolio_return_pct > 0))::double precision / nullif(count(*), 0) * 100 AS daily_win_rate_pct,
        max(portfolio_return_pct) AS best_daily_return_pct,
        min(portfolio_return_pct) AS worst_daily_return_pct
    FROM {{ ref('mart_backtest_portfolio_equity_daily') }}
    GROUP BY backtest_id
),

equity_summary AS (
    SELECT
        *,
        end_date::date - start_date::date AS elapsed_days,
        (final_value / 10000 - 1) * 100 AS total_return_pct,
        (pow(final_value / 10000, 365.25 / nullif(end_date::date - start_date::date, 0)) - 1) * 100 AS annualized_return_pct,
        std_daily_return_pct / 100 * sqrt(252) AS annualized_volatility
    FROM equity_summary_base
),

benchmark_by_backtest AS (
    SELECT
        e.backtest_id,
        (array_agg(i.close ORDER BY i.ts ASC))[1] AS benchmark_start_price,
        (array_agg(i.close ORDER BY i.ts DESC))[1] AS benchmark_end_price
    FROM equity_summary AS e
    CROSS JOIN (
        SELECT *
        FROM {{ ref('int_market_indexes_daily') }}
        WHERE index_id = 'sp500'
    ) AS i
    WHERE i.ts >= e.start_date
      AND i.ts <= e.end_date
    GROUP BY e.backtest_id
),

benchmark_summary AS (
    SELECT
        e.backtest_id,
        ((b.benchmark_end_price / nullif(b.benchmark_start_price, 0)) - 1) * 100 AS benchmark_total_return_pct,
        (pow(b.benchmark_end_price / nullif(b.benchmark_start_price, 0), 365.25 / nullif(e.elapsed_days, 0)) - 1) * 100 AS benchmark_annualized_return_pct
    FROM equity_summary AS e
    INNER JOIN benchmark_by_backtest AS b
        ON e.backtest_id = b.backtest_id
)

SELECT
    e.backtest_id AS backtest_id,
    t.strategy_family AS strategy_family,
    t.rebalance_frequency AS rebalance_frequency,
    t.portfolio_size AS portfolio_size,
    t.universe_scope AS universe_scope,
    t.exposure_policy AS exposure_policy,
    e.start_date AS start_date,
    e.end_date AS end_date,
    e.elapsed_days AS elapsed_days,
    e.final_value AS final_value,
    e.total_return_pct AS total_return_pct,
    e.annualized_return_pct AS annualized_return_pct,
    b.benchmark_total_return_pct AS benchmark_total_return_pct,
    b.benchmark_annualized_return_pct AS benchmark_annualized_return_pct,
    e.annualized_return_pct - b.benchmark_annualized_return_pct AS alpha_pct,
    e.max_drawdown_pct AS max_drawdown_pct,
    e.annualized_volatility * 100 AS annualized_volatility_pct,
    CASE
        WHEN e.annualized_volatility = 0 THEN NULL
        ELSE ((e.annualized_return_pct / 100) - 0.03) / e.annualized_volatility
    END AS sharpe_ratio,
    e.annualized_return_pct / nullif(abs(e.max_drawdown_pct), 0) AS calmar_ratio,
    e.daily_win_rate_pct AS daily_win_rate_pct,
    e.best_daily_return_pct AS best_daily_return_pct,
    e.worst_daily_return_pct AS worst_daily_return_pct,
    t.total_trades AS total_trades,
    t.rebalance_count AS rebalance_count,
    t.avg_gross_trade_return_pct AS avg_gross_trade_return_pct,
    t.avg_net_trade_return_pct AS avg_net_trade_return_pct,
    t.avg_net_trade_return_pct AS avg_trade_return_pct,
    t.median_trade_return_pct AS median_trade_return_pct,
    t.p05_trade_return_pct AS p05_trade_return_pct,
    t.p95_trade_return_pct AS p95_trade_return_pct,
    t.std_trade_return_pct AS std_trade_return_pct,
    t.win_rate * 100 AS win_rate_pct,
    t.profit_factor AS profit_factor,
    t.best_trade_pct AS best_trade_pct,
    t.worst_trade_pct AS worst_trade_pct,
    t.avg_holding_days AS avg_holding_days,
    t.weighted_total_cost_pct AS weighted_total_cost_pct,
    t.weighted_total_cost_pct / nullif(e.elapsed_days, 0) * 365.25 * 100 AS annualized_cost_drag_bps,
    e.annualized_return_pct - 2.0 AS survivorship_bias_adjusted_return_pct,
    CASE
        WHEN e.annualized_volatility = 0 THEN NULL
        ELSE ((e.annualized_return_pct - 2.0) / 100 - 0.03) / e.annualized_volatility
    END AS survivorship_bias_adjusted_sharpe,
    true AS has_survivorship_bias_risk,
    t.avg_actual_position_count AS avg_actual_position_count,
    now() AS computed_at
FROM equity_summary AS e
INNER JOIN trades_summary AS t
    ON e.backtest_id = t.backtest_id
INNER JOIN benchmark_summary AS b
    ON e.backtest_id = b.backtest_id
