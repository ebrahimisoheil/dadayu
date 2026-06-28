{{ config(
    materialized='table',
) }}

WITH weekly_dates AS (
    SELECT
        date_trunc('week', score_date)::timestamp AS week_start,
        max(score_date) AS weekly_score_date
    FROM {{ ref('mart_industry_scores_daily') }}
    WHERE rankable_count > 0
    GROUP BY week_start
),

weekly_scores AS (
    SELECT
        w.week_start,
        s.*
    FROM {{ ref('mart_industry_scores_daily') }} AS s
    INNER JOIN weekly_dates AS w
        ON s.score_date = w.weekly_score_date
    WHERE s.rankable_count > 0
)

SELECT
    *,
    rank() OVER (
        PARTITION BY week_start
        ORDER BY avg_total_score DESC NULLS LAST, rankable_count DESC, industry ASC
    ) AS weekly_industry_rank_overall,
    rank() OVER (
        PARTITION BY week_start, market
        ORDER BY avg_total_score DESC NULLS LAST, rankable_count DESC, industry ASC
    ) AS weekly_industry_rank_in_market,
    rank() OVER (
        PARTITION BY week_start, market, sector
        ORDER BY avg_total_score DESC NULLS LAST, rankable_count DESC, industry ASC
    ) AS weekly_industry_rank_in_sector
FROM weekly_scores
