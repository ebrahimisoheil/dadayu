-- Fails if any signal row exists for a ticker outside its membership span.
SELECT s.ticker, s.market, s.signal_date
FROM {{ ref('mart_backtest_signals_daily') }} AS s
LEFT JOIN {{ ref('int_universe_membership_daily') }} AS m
    ON s.ticker = m.ticker
    AND s.market = m.market
    AND s.signal_date >= m.valid_from
    AND (m.valid_to IS NULL OR s.signal_date < m.valid_to)
WHERE m.ticker IS NULL
