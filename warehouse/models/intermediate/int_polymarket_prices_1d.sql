{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree()',
    order_by='(condition_id, ts)',
    partition_by='toYYYYMM(ts)',
    unique_key=['condition_id', 'ts'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

WITH base AS (
    SELECT
        condition_id,
        toStartOfDay(ts) AS day_ts,
        probability,
        volume_usd
    FROM {{ ref('stg_polymarket__prices') }}
    {% if is_incremental() %}
    -- Reprocess latest in-progress bucket and all newer rows
    WHERE ts >= (SELECT toStartOfDay(max(ts)) FROM {{ this }})
    {% endif %}
)
SELECT
    condition_id,
    day_ts                       AS ts,
    argMax(probability, day_ts)  AS probability,
    sum(volume_usd)              AS volume_usd,
    false                        AS is_interpolated
FROM base
GROUP BY condition_id, day_ts
