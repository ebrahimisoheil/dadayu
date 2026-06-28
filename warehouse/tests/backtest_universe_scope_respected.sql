SELECT
    backtest_id,
    signal_date,
    ticker,
    market,
    universe_scope,
    asset_type
FROM {{ ref('mart_backtest_signals_daily') }}
WHERE universe_scope != 'equity'
   OR asset_type != 'equity'
