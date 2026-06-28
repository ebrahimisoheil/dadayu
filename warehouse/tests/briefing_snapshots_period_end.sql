WITH weekly_expected AS (
    SELECT
        date_trunc('week', score_date)::timestamp AS period_start,
        max(score_date) AS expected_period_end
    FROM {{ ref('mart_briefing_portfolio_daily') }}
    WHERE is_rankable
    GROUP BY period_start
),

monthly_expected AS (
    SELECT
        date_trunc('month', score_date)::timestamp AS period_start,
        max(score_date) AS expected_period_end
    FROM {{ ref('mart_briefing_portfolio_daily') }}
    WHERE is_rankable
    GROUP BY period_start
),

quarterly_expected AS (
    SELECT
        date_trunc('quarter', score_date)::timestamp AS period_start,
        max(score_date) AS expected_period_end
    FROM {{ ref('mart_briefing_portfolio_daily') }}
    WHERE is_rankable
    GROUP BY period_start
),

half_year_expected AS (
    SELECT
        CASE
            WHEN extract(month from score_date) <= 6 THEN date_trunc('year', score_date)::timestamp
            ELSE (date_trunc('year', score_date) + INTERVAL '6 months')::timestamp
        END AS period_start,
        max(score_date) AS expected_period_end
    FROM {{ ref('mart_briefing_portfolio_daily') }}
    WHERE is_rankable
    GROUP BY period_start
)

SELECT
    'weekly' AS mart_name,
    w.week_start AS period_start,
    w.period_end,
    e.expected_period_end
FROM {{ ref('mart_briefing_portfolio_weekly') }} AS w
INNER JOIN weekly_expected AS e
    ON w.week_start = e.period_start
WHERE w.score_date != w.period_end
   OR w.period_end != e.expected_period_end

UNION ALL

SELECT
    'monthly' AS mart_name,
    m.month_start AS period_start,
    m.period_end,
    e.expected_period_end
FROM {{ ref('mart_briefing_portfolio_monthly') }} AS m
INNER JOIN monthly_expected AS e
    ON m.month_start = e.period_start
WHERE m.score_date != m.period_end
   OR m.period_end != e.expected_period_end

UNION ALL

SELECT
    'quarterly' AS mart_name,
    q.quarter_start AS period_start,
    q.period_end,
    e.expected_period_end
FROM {{ ref('mart_briefing_portfolio_quarterly') }} AS q
INNER JOIN quarterly_expected AS e
    ON q.quarter_start = e.period_start
WHERE q.score_date != q.period_end
   OR q.period_end != e.expected_period_end

UNION ALL

SELECT
    '6m' AS mart_name,
    h.half_year_start AS period_start,
    h.period_end,
    e.expected_period_end
FROM {{ ref('mart_briefing_portfolio_6m') }} AS h
INNER JOIN half_year_expected AS e
    ON h.half_year_start = e.period_start
WHERE h.score_date != h.period_end
   OR h.period_end != e.expected_period_end
