{{ config(materialized='table') }}

WITH all_spans AS (
    SELECT ticker, market, valid_from,
           coalesce(valid_to, DATE '9999-12-31') AS valid_to
    FROM {{ ref('seed_index_membership_de') }}
    UNION ALL
    SELECT ticker, market, valid_from,
           coalesce(valid_to, DATE '9999-12-31') AS valid_to
    FROM {{ ref('seed_index_membership_us') }}
    UNION ALL
    SELECT ticker, market, dbt_valid_from::date AS valid_from,
           coalesce(dbt_valid_to::date, DATE '9999-12-31') AS valid_to
    FROM {{ ref('snap_index_membership') }}
),

ordered AS (
    SELECT ticker, market, valid_from, valid_to,
           max(valid_to) OVER (
               PARTITION BY ticker, market
               ORDER BY valid_from, valid_to
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS prev_max_to
    FROM all_spans
),

islands AS (
    SELECT ticker, market, valid_from, valid_to,
           sum(CASE WHEN prev_max_to IS NULL OR valid_from > prev_max_to THEN 1 ELSE 0 END)
               OVER (PARTITION BY ticker, market ORDER BY valid_from, valid_to) AS grp
    FROM ordered
)

SELECT
    ticker,
    market,
    min(valid_from) AS valid_from,
    nullif(max(valid_to), DATE '9999-12-31') AS valid_to
FROM islands
GROUP BY ticker, market, grp
