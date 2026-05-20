{{ config(
    materialized='table',
    engine='ReplacingMergeTree(fetched_at)',
    order_by='(ticker, market)'
) }}

WITH source AS (
    SELECT * FROM {{ source('yahoo', 'tickers') }} FINAL
)

SELECT
    ticker,
    market,
    name,
    sector,
    industry,
    currency,
    country,
    toFloat64OrNull(toString(market_cap))   AS market_cap,
    toFloat64OrNull(toString(pe_ratio))     AS pe_ratio,
    fetched_at
FROM source
