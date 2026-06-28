SELECT *
FROM {{ ref('mart_backtest_trades') }}
WHERE entry_price IS NULL
   OR exit_price IS NULL
   OR entry_date IS NULL
   OR exit_date IS NULL
