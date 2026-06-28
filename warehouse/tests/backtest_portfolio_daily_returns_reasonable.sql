SELECT *
FROM {{ ref('mart_backtest_portfolio_equity_daily') }}
WHERE portfolio_return_pct < -50
   OR portfolio_return_pct > 50

