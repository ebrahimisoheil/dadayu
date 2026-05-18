WITH ohlcv AS (
    SELECT * FROM {{ ref('int_crypto_ohlcv_1h') }}
),

dim AS (
    SELECT yf_symbol, name, category, market_rank
    FROM {{ ref('dim_crypto_symbol') }}
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
    d.name,
    d.category,
    d.market_rank,
    (o.close - lagInFrame(o.close, 1, o.close) OVER w) /
        nullIf(lagInFrame(o.close, 1, o.close) OVER w, 0) AS return_pct
FROM ohlcv AS o
LEFT JOIN dim AS d ON o.ticker = d.yf_symbol
WINDOW w AS (PARTITION BY o.ticker, o.market ORDER BY o.ts)
