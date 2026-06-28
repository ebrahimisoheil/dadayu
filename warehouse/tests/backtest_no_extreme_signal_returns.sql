SELECT *
FROM {{ ref('mart_backtest_signals_daily') }}
WHERE abs(coalesce(return_20d, 0)) > 500
   OR abs(coalesce(return_5d, 0)) > 300

