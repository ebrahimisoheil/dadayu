{{ config(
    materialized='table',
) }}

SELECT
    ticker,
    market,
    name,
    sector,
    industry,
    currency,
    country,
    market_cap,
    pe_ratio,
    fetched_at
FROM {{ ref('snap_dim_equity_symbol') }}
WHERE dbt_valid_to IS NULL
