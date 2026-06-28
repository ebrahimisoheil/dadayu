{{ config(
    materialized='table',
) }}

WITH windows AS (
    SELECT
        'train' AS window_name,
        TIMESTAMP '1970-01-01 00:00:00' AS window_start,
        TIMESTAMP '2024-01-01 00:00:00' AS window_end

    UNION ALL

    SELECT
        'validation' AS window_name,
        TIMESTAMP '2024-01-01 00:00:00' AS window_start,
        TIMESTAMP '2025-01-01 00:00:00' AS window_end

    UNION ALL

    SELECT
        'test' AS window_name,
        TIMESTAMP '2025-01-01 00:00:00' AS window_start,
        TIMESTAMP '2100-01-01 00:00:00' AS window_end

    UNION ALL

    SELECT
        'stress' AS window_name,
        TIMESTAMP '2022-01-01 00:00:00' AS window_start,
        TIMESTAMP '2023-01-01 00:00:00' AS window_end
),

windowed_equity AS (
    SELECT
        e.backtest_id AS backtest_id,
        v.strategy_family AS strategy_family,
        v.rebalance_frequency AS rebalance_frequency,
        v.portfolio_size AS portfolio_size,
        v.universe_scope AS universe_scope,
        v.exposure_policy AS exposure_policy,
        w.window_name AS window_name,
        e.equity_date AS equity_date,
        e.portfolio_return_pct AS portfolio_return_pct,
        e.portfolio_value AS portfolio_value,
        e.drawdown_pct AS drawdown_pct
    FROM {{ ref('mart_backtest_portfolio_equity_daily') }} AS e
    INNER JOIN {{ ref('mart_backtest_strategy_variants') }} AS v
        ON e.backtest_id = v.backtest_id
    CROSS JOIN windows AS w
    WHERE e.equity_date >= w.window_start
      AND e.equity_date < w.window_end
),

summary AS (
    SELECT
        backtest_id,
        strategy_family,
        rebalance_frequency,
        portfolio_size,
        universe_scope,
        exposure_policy,
        window_name,
        min(equity_date) AS window_start_date,
        max(equity_date) AS window_end_date,
        count(*) AS trading_days,
        (array_agg(portfolio_value ORDER BY equity_date ASC))[1] AS start_value,
        (array_agg(portfolio_value ORDER BY equity_date DESC))[1] AS end_value,
        min(drawdown_pct) AS max_drawdown_pct,
        avg(portfolio_return_pct) AS avg_daily_return_pct,
        stddev_pop(portfolio_return_pct) AS std_daily_return_pct,
        (count(*) FILTER (WHERE portfolio_return_pct > 0))::double precision / nullif(count(*), 0) * 100 AS daily_win_rate_pct
    FROM windowed_equity
    GROUP BY
        backtest_id,
        strategy_family,
        rebalance_frequency,
        portfolio_size,
        universe_scope,
        exposure_policy,
        window_name
)

SELECT
    backtest_id,
    strategy_family,
    rebalance_frequency,
    portfolio_size,
    universe_scope,
    exposure_policy,
    window_name,
    window_start_date,
    window_end_date,
    trading_days,
    start_value,
    end_value,
    ((end_value / nullif(start_value, 0)) - 1) * 100 AS window_return_pct,
    max_drawdown_pct,
    avg_daily_return_pct,
    std_daily_return_pct,
    (pow(end_value / nullif(start_value, 0), 365.25 / nullif(window_end_date::date - window_start_date::date, 0)) - 1) * 100 AS annualized_return_pct,
    std_daily_return_pct / 100 * sqrt(252) * 100 AS annualized_volatility_pct,
    CASE
        WHEN std_daily_return_pct = 0 THEN NULL
        ELSE (((pow(end_value / nullif(start_value, 0), 365.25 / nullif(window_end_date::date - window_start_date::date, 0)) - 1) * 100) / 100 - 0.03)
            / (std_daily_return_pct / 100 * sqrt(252))
    END AS sharpe_ratio,
    daily_win_rate_pct
FROM summary
