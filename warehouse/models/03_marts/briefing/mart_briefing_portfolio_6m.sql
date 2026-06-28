{{ config(
    materialized='table',
) }}

WITH last_date_per_period AS (
    SELECT
        CASE
            WHEN extract(month from score_date) <= 6 THEN date_trunc('year', score_date)::timestamp
            ELSE (date_trunc('year', score_date) + INTERVAL '6 months')::timestamp
        END AS half_year_start,
        max(score_date) AS snapshot_date
    FROM {{ ref('mart_briefing_portfolio_daily') }}
    WHERE is_rankable
    GROUP BY half_year_start
)

SELECT
    b.*,
    p.half_year_start,
    p.snapshot_date AS period_end
FROM {{ ref('mart_briefing_portfolio_daily') }} AS b
INNER JOIN last_date_per_period AS p
    ON b.score_date = p.snapshot_date
