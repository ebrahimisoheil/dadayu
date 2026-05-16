WITH source AS (
    SELECT * FROM {{ source('yahoo', 'prices_daily') }}
)

SELECT
    ticker,
    market,
    toDateTime(date)        AS ts,
    open,
    high,
    low,
    close,
    toUInt64(volume)        AS volume
FROM source
