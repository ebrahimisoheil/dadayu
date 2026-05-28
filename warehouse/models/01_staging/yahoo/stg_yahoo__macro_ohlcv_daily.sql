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
    SELECT * FROM {{ source('yahoo', 'macro_prices_daily') }} AS source_raw
    {% if is_incremental() %}
    WHERE date > (SELECT COALESCE(MAX(ts)::date, DATE '1900-01-01') FROM {{ this }})
       OR NOT EXISTS (
            SELECT 1
            FROM {{ this }} AS target
            WHERE target.ticker = source_raw.ticker
              AND target.market = source_raw.market
              AND target.ts = source_raw.date::timestamp
       )
    {% endif %}
),

universe AS (
    SELECT ticker, name, instrument_type, regime_dimension
    FROM {{ ref('macro_universe') }}
)

SELECT
    s.ticker,
    s.market,
    s.date::timestamp AS ts,
    s.open,
    s.high,
    s.low,
    s.close,
    COALESCE(s.volume, 0) AS volume,
    u.name,
    u.instrument_type,
    u.regime_dimension,
    s.ingested_at
FROM source AS s
LEFT JOIN universe AS u ON s.ticker = u.ticker
WHERE s.close > 0
