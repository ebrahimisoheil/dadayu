WITH ohlcv AS (
    SELECT * FROM {{ ref('int_crypto_ohlcv_1d') }}
),

dim AS (
    SELECT
        u.symbol     AS yf_symbol,
        d.name,
        d.category,
        d.market_rank
    FROM {{ ref('dim_crypto_symbol') }} AS d
    JOIN {{ ref('crypto_universe') }} AS u ON d.coin_id = u.coingecko_id
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
