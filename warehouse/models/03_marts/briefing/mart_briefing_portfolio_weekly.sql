{{ config(
    materialized='table',
) }}

WITH last_date_per_period AS (
    SELECT
        date_trunc('week', score_date)::timestamp AS week_start,
        max(score_date) AS snapshot_date
    FROM {{ ref('mart_briefing_portfolio_daily') }}
    WHERE is_rankable
    GROUP BY week_start
)

SELECT
    b.*,
    p.week_start,
    p.snapshot_date AS period_end
FROM {{ ref('mart_briefing_portfolio_daily') }} AS b
INNER JOIN last_date_per_period AS p
    ON b.score_date = p.snapshot_date
