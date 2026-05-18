{% snapshot snap_dim_crypto_symbol %}

{{ config(
    target_schema='dadayu',
    unique_key='coin_id',
    strategy='check',
    check_cols=['market_rank', 'category']
) }}

SELECT
    coin_id,
    symbol,
    name,
    market_rank,
    market_cap,
    category,
    chain,
    fetched_at
FROM {{ ref('stg_coingecko__crypto_info') }}

{% endsnapshot %}
