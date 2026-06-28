SELECT *
FROM {{ ref('mart_backtest_performance') }}
WHERE benchmark_annualized_return_pct IS NULL
   OR benchmark_total_return_pct IS NULL
