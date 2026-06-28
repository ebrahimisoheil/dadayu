{{ config(
    materialized='table',
) }}

WITH source AS (
    SELECT * FROM {{ source('yahoo', 'tickers') }}
)

SELECT
    ticker,
    market,
    name,
    sector,
    industry,
    currency,
    country,
    (market_cap)::double precision AS market_cap,
    (pe_ratio)::double precision AS pe_ratio,
    fetched_at
FROM source
WHERE market IN ('us', 'germany')

