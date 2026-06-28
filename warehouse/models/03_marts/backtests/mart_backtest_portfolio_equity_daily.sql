{{ config(
    materialized='table',
) }}

WITH mark_prices AS (
    SELECT
        t.backtest_id,
        t.exposure_policy,
        t.entry_date,
        t.exit_date,
        t.ticker,
        t.market,
        t.position_weight,
        p.ts AS equity_date,
        p.close AS mark_price,
        t.entry_net_price AS entry_price
    FROM {{ ref('mart_backtest_trades') }} AS t
    INNER JOIN {{ ref('int_market_backtest_prices_daily') }} AS p
        ON t.ticker = p.ticker
        AND t.market = p.market
    WHERE p.ts >= t.entry_date
      AND p.ts < t.exit_date
      AND p.is_backtest_tradable
),

exit_prices AS (
    SELECT
        backtest_id,
        exposure_policy,
        entry_date,
        exit_date,
        ticker,
        market,
        position_weight,
        exit_date AS equity_date,
        exit_net_price AS mark_price,
        entry_net_price AS entry_price
    FROM {{ ref('mart_backtest_trades') }}
),

position_prices AS (
    SELECT
        backtest_id,
        exposure_policy,
        entry_date,
        exit_date,
        ticker,
        market,
        position_weight,
        equity_date,
        mark_price,
        entry_price
    FROM mark_prices

    UNION ALL

    SELECT
        backtest_id,
        exposure_policy,
        entry_date,
        exit_date,
        ticker,
        market,
        position_weight,
        equity_date,
        mark_price,
        entry_price
    FROM exit_prices
),

position_returns AS (
    SELECT
        *,
        ((mark_price / nullif(coalesce(lag(mark_price, 1) OVER (
                PARTITION BY backtest_id, entry_date, ticker, market
                ORDER BY equity_date
            ), entry_price), 0)) - 1) * 100 AS position_return_pct
    FROM position_prices
),

daily_returns_base AS (
    SELECT
        backtest_id,
        (array_agg(exposure_policy))[1] AS exposure_policy,
        equity_date,
        count(DISTINCT (ticker, market)) AS held_position_count,
        sum(position_weight) AS raw_active_position_weight,
        sum(position_weight * position_return_pct) AS raw_portfolio_return_pct
    FROM position_returns
    GROUP BY backtest_id, equity_date
),

daily_returns_with_multiplier AS (
    SELECT
        d.backtest_id,
        d.exposure_policy,
        d.equity_date,
        coalesce(r.market_regime, 'unknown') AS market_regime,
        CASE
            WHEN d.exposure_policy = 'full' THEN 1.0
            WHEN coalesce(r.market_regime, 'neutral') = 'risk_on' THEN 1.0
            WHEN coalesce(r.market_regime, 'neutral') = 'risk_off' THEN 0.3
            ELSE 0.7
        END AS exposure_multiplier,
        d.held_position_count,
        d.raw_active_position_weight,
        d.raw_portfolio_return_pct
    FROM daily_returns_base AS d
    LEFT JOIN {{ ref('mart_market_regime_daily') }} AS r
        ON d.equity_date = r.ts
),

daily_returns AS (
    SELECT
        *,
        least(raw_active_position_weight, 1.0) * exposure_multiplier AS active_position_weight,
        (raw_portfolio_return_pct / greatest(raw_active_position_weight, 1.0)) * exposure_multiplier AS portfolio_return_pct
    FROM daily_returns_with_multiplier
),

compounded AS (
    SELECT
        backtest_id,
        exposure_policy,
        equity_date,
        market_regime,
        exposure_multiplier,
        held_position_count,
        active_position_weight,
        portfolio_return_pct,
        10000 * exp(
            sum(ln(greatest(0.000001, 1 + portfolio_return_pct / 100))) OVER (
                PARTITION BY backtest_id
                ORDER BY equity_date
            )
        ) AS portfolio_value
    FROM daily_returns
),

with_drawdown AS (
    SELECT
        *,
        (portfolio_value / 10000 - 1) * 100 AS cumulative_return_pct,
        (portfolio_value / nullif(max(portfolio_value) OVER (
            PARTITION BY backtest_id
            ORDER BY equity_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 0) - 1) * 100 AS drawdown_pct
    FROM compounded
)

SELECT
    backtest_id,
    exposure_policy,
    equity_date,
    market_regime,
    exposure_multiplier,
    held_position_count,
    active_position_weight,
    portfolio_return_pct,
    cumulative_return_pct,
    portfolio_value,
    drawdown_pct
FROM with_drawdown
