SELECT *
FROM {{ ref('int_market_data_quality_daily') }}
WHERE is_backtest_tradable
  AND NOT is_valid_market_data

