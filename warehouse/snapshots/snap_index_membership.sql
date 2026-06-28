{% snapshot snap_index_membership %}

{{ config(
    target_schema='dadayu',
    unique_key=dbt_utils.generate_surrogate_key(['ticker', 'market', 'index_name']),
    strategy='check',
    check_cols=['index_name'],
    invalidate_hard_deletes=true
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ticker', 'market', 'index_name']) }} AS membership_id,
    ticker,
    market,
    index_name,
    observed_at
FROM {{ ref('stg_membership__observed') }}

{% endsnapshot %}
