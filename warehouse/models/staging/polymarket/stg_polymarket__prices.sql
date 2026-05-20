{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(ingested_at)',
    order_by='(condition_id, ts)',
    partition_by='toYYYYMM(ts)',
    unique_key=['condition_id', 'ts'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('polymarket', 'polymarket_prices') }} FINAL
    {% if is_incremental() %}
    WHERE ingested_at > (SELECT max(ingested_at) FROM {{ this }})
    {% endif %}
)

SELECT
    condition_id,
    ts,
    probability,
    volume_usd,
    ingested_at
FROM source
WHERE probability BETWEEN 0 AND 1
