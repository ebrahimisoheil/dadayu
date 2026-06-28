{{ config(
    materialized='table',
) }}

SELECT
    e.ticker,
    e.market,
    'equity' AS asset_type,
    e.ts,
    e.open,
    e.high,
    e.low,
    e.close,
    e.volume,
    m.name,
    m.sector,
    m.industry,
    m.market_cap,
    (e.close - coalesce(lag(e.close, 1) OVER w_equity, e.close))
        / nullif(coalesce(lag(e.close, 1) OVER w_equity, e.close), 0) AS return_pct
FROM {{ ref('stg_yahoo__equity_ohlcv_daily') }} AS e
LEFT JOIN {{ ref('int_reference_equity_symbols_current') }} AS m
    ON e.ticker = m.ticker
    AND e.market = m.market
WINDOW w_equity AS (PARTITION BY e.ticker, e.market ORDER BY e.ts)
