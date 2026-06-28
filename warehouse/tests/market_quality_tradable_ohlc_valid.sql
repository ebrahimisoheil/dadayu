SELECT *
FROM {{ ref('int_market_data_quality_daily') }}
WHERE is_backtest_tradable
  AND NOT has_valid_ohlc

