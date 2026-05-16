WITH source AS (
    SELECT * FROM {{ source('yahoo', 'prices_4h') }}
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
