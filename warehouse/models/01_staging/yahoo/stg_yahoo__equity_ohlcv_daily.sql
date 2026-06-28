{{ config(
    materialized='incremental',
    unique_key=['ticker', 'market', 'ts'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    indexes=[
        {'columns': ['ticker', 'market', 'ts'], 'unique': True},
    ],
) }}

WITH source AS (
    SELECT * FROM {{ source('yahoo', 'prices_daily') }} AS source_raw
    {% if is_incremental() %}
    WHERE date > (SELECT coalesce(max(ts)::date, DATE '1900-01-01') FROM {{ this }})
       OR NOT EXISTS (
            SELECT 1
            FROM {{ this }} AS target
            WHERE target.ticker = source_raw.ticker
              AND target.market = source_raw.market
              AND target.ts = source_raw.date::timestamp
       )
    {% endif %}
)

SELECT
    ticker,
    market,
    date::timestamp AS ts,
    open,
    high,
    low,
    close,
    volume,
    ingested_at
FROM source
WHERE market IN ('us', 'germany')
  AND close > 0
