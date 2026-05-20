{{ config(
    materialized='table',
    engine='ReplacingMergeTree(fetched_at)',
    order_by='condition_id'
) }}

WITH source AS (
    SELECT * FROM {{ source('polymarket', 'polymarket_markets') }} FINAL
)

SELECT
    condition_id,
    question,
    category,
    volume_usd,
    liquidity_usd,
    active,
    closed,
    resolution_date,
    outcome,
    yes_token_id,
    linked_asset,
    asset_type,
    fetched_at
FROM source
