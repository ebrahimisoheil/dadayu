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
        toStartOfHour(ts) AS hour_ts,
        probability,
        volume_usd
    FROM {{ ref('stg_polymarket__prices') }}
    {% if is_incremental() %}
    -- Reprocess latest in-progress bucket and all newer rows
    WHERE ts >= (SELECT toStartOfHour(max(ts)) FROM {{ this }})
    {% endif %}
)
SELECT
    condition_id,
    hour_ts                       AS ts,
    argMax(probability, hour_ts)  AS probability,
    sum(volume_usd)               AS volume_usd,
    false                         AS is_interpolated
FROM base
GROUP BY condition_id, hour_ts
