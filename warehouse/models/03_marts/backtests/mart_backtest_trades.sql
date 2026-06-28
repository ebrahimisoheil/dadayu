{{ config(
    materialized='table',
) }}

WITH all_signals AS (
    SELECT
        backtest_id,
        strategy_family,
        rebalance_frequency,
        portfolio_size,
        universe_scope,
        exposure_policy,
        signal_date,
        ticker,
        market,
        asset_type,
        close,
        avg_dollar_volume_20d,
        base_total_score,
        total_score,
        risk_bucket,
        signal_rank
    FROM {{ ref('mart_backtest_signals_daily') }}
),

rebalance_dates AS (
    SELECT
        backtest_id,
        rebalance_frequency,
        CASE
            WHEN rebalance_frequency = 'daily' THEN signal_date
            WHEN rebalance_frequency = 'weekly' THEN date_trunc('week', signal_date)::timestamp
            WHEN rebalance_frequency = 'monthly' THEN date_trunc('month', signal_date)::timestamp
            WHEN rebalance_frequency = 'quarterly' THEN date_trunc('quarter', signal_date)::timestamp
            WHEN rebalance_frequency = '6m' AND extract(month from signal_date) <= 6 THEN date_trunc('year', signal_date)::timestamp
            WHEN rebalance_frequency = '6m' THEN (date_trunc('year', signal_date) + INTERVAL '6 months')::timestamp
            ELSE signal_date
        END AS rebalance_period_start,
        max(signal_date) AS rebalance_signal_date
    FROM all_signals
    GROUP BY backtest_id, rebalance_frequency, rebalance_period_start
),

rebalance_schedule AS (
    SELECT
        backtest_id,
        rebalance_frequency,
        rebalance_period_start,
        rebalance_signal_date,
        lead(rebalance_signal_date, 1) OVER (
            PARTITION BY backtest_id
            ORDER BY rebalance_signal_date
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS next_rebalance_signal_date
    FROM rebalance_dates
),

selected_signals AS (
    SELECT
        s.backtest_id,
        s.strategy_family,
        s.rebalance_frequency,
        s.portfolio_size,
        s.universe_scope,
        s.exposure_policy,
        r.rebalance_period_start,
        r.rebalance_signal_date AS rebalance_date,
        r.next_rebalance_signal_date,
        s.ticker,
        s.market,
        s.asset_type,
        s.risk_bucket,
        s.signal_rank,
        s.total_score,
        s.base_total_score,
        s.avg_dollar_volume_20d,
        count(*) OVER (
            PARTITION BY s.backtest_id, s.signal_date
        ) AS selected_position_count
    FROM all_signals AS s
    INNER JOIN rebalance_schedule AS r
        ON s.backtest_id = r.backtest_id
        AND s.signal_date = r.rebalance_signal_date
    WHERE r.next_rebalance_signal_date IS NOT NULL
),

entries AS (
    SELECT
        s.backtest_id,
        s.strategy_family,
        s.rebalance_frequency,
        s.portfolio_size,
        s.universe_scope,
        s.exposure_policy,
        s.rebalance_period_start,
        s.rebalance_date,
        s.next_rebalance_signal_date,
        s.ticker,
        s.market,
        s.asset_type,
        s.risk_bucket,
        s.signal_rank,
        s.total_score,
        s.base_total_score,
        s.avg_dollar_volume_20d,
        s.selected_position_count,
        min(p.ts) AS entry_date,
        (array_agg(p.open ORDER BY p.ts ASC))[1] AS entry_price
    FROM selected_signals AS s
    INNER JOIN {{ ref('int_market_backtest_prices_daily') }} AS p
        ON s.ticker = p.ticker
        AND s.market = p.market
    WHERE p.ts > s.rebalance_date
      AND p.ts < s.next_rebalance_signal_date
      AND p.is_backtest_tradable
    GROUP BY
        s.backtest_id,
        s.strategy_family,
        s.rebalance_frequency,
        s.portfolio_size,
        s.universe_scope,
        s.exposure_policy,
        s.rebalance_period_start,
        s.rebalance_date,
        s.next_rebalance_signal_date,
        s.ticker,
        s.market,
        s.asset_type,
        s.risk_bucket,
        s.signal_rank,
        s.total_score,
        s.base_total_score,
        s.avg_dollar_volume_20d,
        s.selected_position_count
),

exits AS (
    SELECT
        e.backtest_id,
        e.strategy_family,
        e.rebalance_frequency,
        e.portfolio_size,
        e.universe_scope,
        e.exposure_policy,
        e.rebalance_period_start,
        e.rebalance_date,
        e.ticker,
        e.market,
        e.asset_type,
        e.risk_bucket,
        e.signal_rank,
        e.total_score,
        e.base_total_score,
        e.avg_dollar_volume_20d,
        e.selected_position_count,
        e.entry_date,
        e.entry_price,
        min(p.ts) AS exit_date,
        (array_agg(p.open ORDER BY p.ts ASC))[1] AS exit_price
    FROM entries AS e
    INNER JOIN {{ ref('int_market_backtest_prices_daily') }} AS p
        ON e.ticker = p.ticker
        AND e.market = p.market
    WHERE p.ts > greatest(e.next_rebalance_signal_date, e.entry_date)
      AND p.ts <= e.next_rebalance_signal_date + INTERVAL '10 days'
      AND p.is_backtest_tradable
    GROUP BY
        e.backtest_id,
        e.strategy_family,
        e.rebalance_frequency,
        e.portfolio_size,
        e.universe_scope,
        e.exposure_policy,
        e.rebalance_period_start,
        e.rebalance_date,
        e.ticker,
        e.market,
        e.asset_type,
        e.risk_bucket,
        e.signal_rank,
        e.total_score,
        e.base_total_score,
        e.avg_dollar_volume_20d,
        e.selected_position_count,
        e.entry_date,
        e.entry_price
),

costed AS (
    SELECT
        *,
        20.0 AS entry_cost_bps,
        20.0 AS exit_cost_bps,
        5.0 + CASE WHEN avg_dollar_volume_20d < 25000000 THEN 10.0 ELSE 0.0 END AS entry_slippage_bps,
        5.0 + CASE WHEN avg_dollar_volume_20d < 25000000 THEN 10.0 ELSE 0.0 END AS exit_slippage_bps
    FROM exits
    WHERE entry_price IS NOT NULL
      AND exit_price IS NOT NULL
),

net_prices AS (
    SELECT
        *,
        entry_price * (1 + (entry_cost_bps + entry_slippage_bps) / 10000) AS entry_net_price,
        exit_price * (1 - (exit_cost_bps + exit_slippage_bps) / 10000) AS exit_net_price,
        ((exit_price / nullif(entry_price, 0)) - 1) * 100 AS gross_return_pct,
        entry_cost_bps / 100 AS entry_cost_pct,
        exit_cost_bps / 100 AS exit_cost_pct,
        (entry_slippage_bps + exit_slippage_bps) / 100 AS slippage_pct
    FROM costed
),

returns AS (
    SELECT
        *,
        ((exit_net_price / nullif(entry_net_price, 0)) - 1) * 100 AS net_return_pct
    FROM net_prices
)

SELECT
    backtest_id,
    strategy_family,
    rebalance_frequency,
    portfolio_size,
    universe_scope,
    exposure_policy,
    rebalance_period_start,
    rebalance_date,
    entry_date,
    exit_date,
    ticker,
    market,
    asset_type,
    risk_bucket,
    signal_rank,
    total_score,
    base_total_score,
    avg_dollar_volume_20d,
    entry_price,
    exit_price,
    entry_net_price,
    exit_net_price,
    gross_return_pct,
    net_return_pct,
    gross_return_pct - net_return_pct AS total_cost_pct,
    entry_cost_pct,
    exit_cost_pct,
    slippage_pct,
    net_return_pct AS return_pct,
    1.0 / count(*) OVER (
        PARTITION BY backtest_id, rebalance_date
    ) AS position_weight,
    count(*) OVER (
        PARTITION BY backtest_id, rebalance_date
    ) AS actual_position_count,
    selected_position_count,
    exit_date::date - entry_date::date AS holding_days
FROM returns
