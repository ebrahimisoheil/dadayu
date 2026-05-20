{% snapshot snap_dim_polymarket_market %}

{{ config(
    target_schema='dadayu',
    unique_key='condition_id',
    strategy='check',
    check_cols=['active', 'closed', 'outcome', 'volume_usd', 'liquidity_usd', 'linked_asset', 'asset_type']
) }}

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
FROM {{ ref('stg_polymarket__markets') }}

{% endsnapshot %}
