SELECT *
FROM {{ ref('mart_backtest_trades') }}
WHERE entry_date <= rebalance_date
   OR exit_date <= entry_date
   OR entry_price <= 0
   OR exit_price <= 0
