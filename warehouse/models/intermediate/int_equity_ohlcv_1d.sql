WITH ohlcv AS (
    SELECT * FROM {{ ref('stg_yahoo__ohlcv_1d') }}
),

calendar AS (
    SELECT * FROM {{ ref('int_calendar_sessions') }}
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
    c.session_id,
    c.is_trading_day,
    c.session_open_utc,
    c.session_close_utc
FROM ohlcv AS o
LEFT JOIN calendar AS c
    ON toDate(o.ts) = c.date
    AND o.market = c.market
