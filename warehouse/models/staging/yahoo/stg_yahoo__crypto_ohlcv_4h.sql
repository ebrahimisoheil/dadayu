WITH source AS (
    SELECT * FROM {{ source('yahoo', 'crypto_prices_4h') }} FINAL
)

SELECT
    ticker,
    market,
    datetime                AS ts,
    open,
    high,
    low,
    close,
    toUInt64(volume)        AS volume
FROM source
