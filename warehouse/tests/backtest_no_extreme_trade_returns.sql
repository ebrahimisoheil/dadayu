SELECT *
FROM {{ ref('mart_backtest_trades') }}
WHERE abs(return_pct) > 1000

