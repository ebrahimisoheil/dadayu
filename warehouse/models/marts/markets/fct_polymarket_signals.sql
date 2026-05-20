{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(condition_id, ts)',
    partition_by='toYYYYMM(ts)'
) }}

WITH hourly AS (
    SELECT * FROM {{ ref('int_polymarket_prices_1h') }}
),

markets AS (
    SELECT
        condition_id,
        question,
        linked_asset,
        asset_type,
        resolution_date
    FROM {{ ref('stg_polymarket__markets') }}
)

SELECT
    h.condition_id,
    h.ts,
    h.probability,
    h.probability - lagInFrame(h.probability, 1, h.probability) OVER w  AS prob_change,
    log(
        greatest(least(h.probability, 0.99), 0.01)
        / (1.0 - greatest(least(h.probability, 0.99), 0.01))
    )                                                                    AS log_odds,
    h.volume_usd,
    h.is_interpolated,
    m.question,
    m.linked_asset,
    m.asset_type,
    if(
        m.resolution_date IS NOT NULL,
        dateDiff('day', h.ts, toDateTime(m.resolution_date)),
        NULL
    )                                                                    AS days_to_resolution
FROM hourly AS h
LEFT JOIN markets AS m USING (condition_id)
WINDOW w AS (PARTITION BY h.condition_id ORDER BY h.ts)
