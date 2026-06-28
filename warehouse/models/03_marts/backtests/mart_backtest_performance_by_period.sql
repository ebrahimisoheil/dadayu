{{ config(
    materialized='table',
) }}

WITH periodized AS (
    SELECT
        p.backtest_id AS backtest_id,
        v.strategy_family AS strategy_family,
        v.rebalance_frequency AS rebalance_frequency,
        v.portfolio_size AS portfolio_size,
        v.universe_scope AS universe_scope,
        v.exposure_policy AS exposure_policy,
        'month' AS period_type,
        date_trunc('month', p.equity_date)::timestamp AS period_start,
        p.equity_date AS equity_date,
        p.portfolio_return_pct AS portfolio_return_pct,
        p.portfolio_value AS portfolio_value,
        p.drawdown_pct AS drawdown_pct
    FROM {{ ref('mart_backtest_portfolio_equity_daily') }} AS p
    INNER JOIN {{ ref('mart_backtest_strategy_variants') }} AS v
        ON p.backtest_id = v.backtest_id

    UNION ALL

    SELECT
        p.backtest_id AS backtest_id,
        v.strategy_family AS strategy_family,
        v.rebalance_frequency AS rebalance_frequency,
        v.portfolio_size AS portfolio_size,
        v.universe_scope AS universe_scope,
        v.exposure_policy AS exposure_policy,
        'year' AS period_type,
        date_trunc('year', p.equity_date)::timestamp AS period_start,
        p.equity_date AS equity_date,
        p.portfolio_return_pct AS portfolio_return_pct,
        p.portfolio_value AS portfolio_value,
        p.drawdown_pct AS drawdown_pct
    FROM {{ ref('mart_backtest_portfolio_equity_daily') }} AS p
    INNER JOIN {{ ref('mart_backtest_strategy_variants') }} AS v
        ON p.backtest_id = v.backtest_id
)

,
aggregated AS (
SELECT
    backtest_id,
    strategy_family,
    rebalance_frequency,
    portfolio_size,
    universe_scope,
    exposure_policy,
    period_type,
    period_start,
    min(equity_date) AS period_first_date,
    max(equity_date) AS period_last_date,
    count(*) AS trading_days,
    (array_agg(portfolio_value ORDER BY equity_date ASC))[1] AS period_start_value,
    (array_agg(portfolio_value ORDER BY equity_date DESC))[1] AS period_end_value,
    min(drawdown_pct) AS max_drawdown_pct,
    avg(portfolio_return_pct) AS avg_daily_return_pct,
    stddev_pop(portfolio_return_pct) AS std_daily_return_pct,
    (count(*) FILTER (WHERE portfolio_return_pct > 0))::double precision / nullif(count(*), 0) * 100 AS daily_win_rate_pct
FROM periodized
GROUP BY
    backtest_id,
    strategy_family,
    rebalance_frequency,
    portfolio_size,
    universe_scope,
    exposure_policy,
    period_type,
    period_start
)

SELECT
    *,
    ((period_end_value / nullif(period_start_value, 0)) - 1) * 100 AS period_return_pct
FROM aggregated
