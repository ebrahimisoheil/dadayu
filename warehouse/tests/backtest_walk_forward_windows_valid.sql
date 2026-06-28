SELECT
    backtest_id,
    window_name,
    window_start_date,
    window_end_date,
    trading_days
FROM {{ ref('mart_backtest_walk_forward_performance') }}
WHERE window_end_date < window_start_date
   OR trading_days <= 0
   OR window_name NOT IN ('train', 'validation', 'test', 'stress')
