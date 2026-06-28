SELECT *
FROM {{ ref('mart_backtest_trades') }}
WHERE gross_return_pct > 0
  AND net_return_pct > gross_return_pct

