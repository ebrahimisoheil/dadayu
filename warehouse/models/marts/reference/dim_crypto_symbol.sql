WITH snapshot AS (
    SELECT *
    FROM {{ ref('snap_dim_crypto_symbol') }}
    WHERE dbt_valid_to IS NULL
),

universe AS (
    SELECT symbol, coingecko_id
    FROM {{ ref('crypto_universe') }}
)

SELECT
    s.coin_id,
    s.symbol,
    s.name,
    s.market_rank,
    s.market_cap,
    s.category,
    s.chain,
    u.symbol        AS yf_symbol,
    s.fetched_at
FROM snapshot AS s
LEFT JOIN universe AS u ON s.coin_id = u.coingecko_id
