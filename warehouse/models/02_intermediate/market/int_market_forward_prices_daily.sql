{{ config(
    materialized='table',
) }}

SELECT
    ticker,
    market,
    ts AS signal_date,
    close,
    lead(close, 5) OVER w AS close_5d_forward,
    lead(close, 20) OVER w AS close_20d_forward,
    lead(close, 60) OVER w AS close_60d_forward
FROM {{ ref('int_market_backtest_prices_daily') }}
WHERE ts >= (current_date - INTERVAL '5 years')::timestamp
  AND ts < current_date::timestamp
  AND is_backtest_tradable
WINDOW w AS (
    PARTITION BY ticker, market
    ORDER BY ts
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
)
