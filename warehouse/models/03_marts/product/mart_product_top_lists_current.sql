{{ config(
    materialized='table',
) }}

WITH latest_date AS (
    SELECT max(score_date) AS score_date
    FROM {{ ref('mart_product_stock_recommendations_daily') }}
),

latest AS (
    SELECT r.*
    FROM {{ ref('mart_product_stock_recommendations_daily') }} AS r
    INNER JOIN latest_date AS d
        ON r.score_date = d.score_date
    WHERE r.product_rank <= 30
),

lists AS (
    SELECT 'top_10' AS list_name, 10 AS max_rank
    UNION ALL
    SELECT 'top_20' AS list_name, 20 AS max_rank
    UNION ALL
    SELECT 'top_30' AS list_name, 30 AS max_rank
)

SELECT
    l.list_name,
    r.score_date AS as_of_date,
    r.product_rank AS list_rank,
    r.ticker,
    r.market,
    r.name,
    r.sector,
    r.industry,
    r.close,
    r.total_score,
    r.risk_bucket,
    r.action_bucket,
    r.primary_signal_reason,
    r.risk_note,
    r.product_disclaimer
FROM latest AS r
INNER JOIN lists AS l
    ON r.product_rank <= l.max_rank
