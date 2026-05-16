{% snapshot snap_dim_equity_symbol %}

{{ config(
    target_schema='dadayu',
    unique_key=dbt_utils.generate_surrogate_key(['ticker', 'market']),
    strategy='check',
    check_cols=['name', 'sector', 'industry', 'market_cap']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ticker', 'market']) }} AS equity_id,
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
FROM {{ ref('stg_yahoo__ticker_info') }}

{% endsnapshot %}
