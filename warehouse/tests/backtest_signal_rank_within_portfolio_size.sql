SELECT *
FROM {{ ref('mart_backtest_signals_daily') }}
WHERE signal_rank > portfolio_size
