WITH ohlcv AS (
    SELECT * FROM {{ ref('int_equity_ohlcv_1d') }}
),

dim AS (
    SELECT ticker, market, name, sector, industry
    FROM {{ ref('dim_equity_symbol') }}
)

SELECT
    o.ticker,
    o.market,
    o.ts,
    o.open,
    o.high,
    o.low,
    o.close,
    o.volume,
    o.session_id,
    o.is_trading_day,
    o.session_open_utc,
    o.session_close_utc,
    d.name,
    d.sector,
    d.industry,
    (o.close - lagInFrame(o.close, 1, o.close) OVER w) /
        nullIf(lagInFrame(o.close, 1, o.close) OVER w, 0) AS return_pct
FROM ohlcv AS o
LEFT JOIN dim AS d USING (ticker, market)
WINDOW w AS (PARTITION BY o.ticker, o.market ORDER BY o.ts)
