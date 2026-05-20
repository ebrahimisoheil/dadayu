SELECT
    condition_id,
    toStartOfDay(ts)        AS ts,
    argMax(probability, ts) AS probability,
    sum(volume_usd)         AS volume_usd,
    false                   AS is_interpolated
FROM {{ ref('stg_polymarket__prices') }}
GROUP BY condition_id, toStartOfDay(ts)
