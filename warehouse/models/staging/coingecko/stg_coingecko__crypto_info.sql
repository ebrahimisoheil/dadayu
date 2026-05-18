WITH source AS (
    SELECT * FROM {{ source('coingecko', 'crypto_metadata') }} FINAL
)

SELECT
    coin_id,
    symbol,
    name,
    rank                        AS market_rank,
    CAST(market_cap AS Nullable(Float64)) AS market_cap,
    category,
    chain,
    fetched_at
FROM source
