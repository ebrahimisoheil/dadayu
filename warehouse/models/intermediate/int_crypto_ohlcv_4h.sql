SELECT
    ticker,
    market,
    ts,
    open,
    high,
    low,
    close,
    volume
FROM {{ ref('stg_yahoo__crypto_ohlcv_4h') }}
