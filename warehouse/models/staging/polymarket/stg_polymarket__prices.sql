WITH source AS (
    SELECT * FROM {{ source('polymarket', 'polymarket_prices') }} FINAL
)

SELECT
    condition_id,
    ts,
    probability,
    volume_usd,
    ingested_at
FROM source
WHERE probability BETWEEN 0 AND 1
