WITH base AS (
    SELECT
        condition_id,
        toStartOfHour(ts) AS hour_ts,
        probability,
        volume_usd
    FROM {{ ref('stg_polymarket__prices') }}
)
SELECT
    condition_id,
    hour_ts                       AS ts,
    argMax(probability, hour_ts)  AS probability,
    sum(volume_usd)               AS volume_usd,
    false                         AS is_interpolated
FROM base
GROUP BY condition_id, hour_ts
