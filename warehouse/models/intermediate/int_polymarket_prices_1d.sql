WITH base AS (
    SELECT
        condition_id,
        toStartOfDay(ts) AS day_ts,
        probability,
        volume_usd
    FROM {{ ref('stg_polymarket__prices') }}
)
SELECT
    condition_id,
    day_ts                       AS ts,
    argMax(probability, day_ts)  AS probability,
    sum(volume_usd)              AS volume_usd,
    false                        AS is_interpolated
FROM base
GROUP BY condition_id, day_ts
