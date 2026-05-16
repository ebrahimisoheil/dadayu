WITH source AS (
    SELECT * FROM {{ source('yahoo', 'prices_hourly') }}
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
