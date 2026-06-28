{{ config(
    materialized='table',
) }}

WITH last_date_per_period AS (
    SELECT
        date_trunc('month', score_date)::timestamp AS month_start,
        max(score_date) AS snapshot_date
    FROM {{ ref('mart_briefing_portfolio_daily') }}
    WHERE is_rankable
    GROUP BY month_start
)

SELECT
    b.*,
    p.month_start,
    p.snapshot_date AS period_end
FROM {{ ref('mart_briefing_portfolio_daily') }} AS b
INNER JOIN last_date_per_period AS p
    ON b.score_date = p.snapshot_date
