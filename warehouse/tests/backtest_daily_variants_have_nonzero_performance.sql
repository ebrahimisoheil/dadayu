SELECT *
FROM {{ ref('mart_backtest_performance') }}
WHERE backtest_id LIKE '%_daily_top%'
  AND elapsed_days >= 30
  AND abs(coalesce(total_return_pct, 0)) < 0.001
