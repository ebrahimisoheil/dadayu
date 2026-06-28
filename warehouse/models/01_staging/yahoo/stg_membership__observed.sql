{{ config(materialized='view') }}

SELECT
    ticker,
    market,
    index_name,
    observed_at
FROM {{ source('yahoo', 'index_membership_observed') }}
