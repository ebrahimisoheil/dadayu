{{ config(
    materialized='table',
) }}

WITH weekly_dates AS (
    SELECT
        date_trunc('week', score_date)::timestamp AS week_start,
        max(score_date) AS weekly_score_date
    FROM {{ ref('mart_product_stock_recommendations_daily') }}
    GROUP BY week_start
)

SELECT
    w.week_start,
    r.*,
    rank() OVER (
        PARTITION BY w.week_start, r.market
        ORDER BY r.total_score DESC NULLS LAST, r.momentum_12_1_pct DESC NULLS LAST
    ) AS weekly_product_rank
FROM {{ ref('mart_product_stock_recommendations_daily') }} AS r
INNER JOIN weekly_dates AS w
    ON r.score_date = w.weekly_score_date
