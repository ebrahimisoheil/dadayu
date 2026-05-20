{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree()',
    order_by='(ticker, market, ts)',
    partition_by='toYYYYMM(ts)',
    unique_key=['ticker', 'market', 'ts'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('yahoo', 'prices_daily') }} FINAL
    {% if is_incremental() %}
    WHERE date > (SELECT toDate(max(ts)) FROM {{ this }})
    {% endif %}
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
