WITH snapshot AS (
    SELECT *
    FROM {{ ref('snap_dim_equity_symbol') }}
    WHERE dbt_valid_to IS NULL
),

gics AS (
    SELECT
        sector_name,
        any(sector_id)             AS sector_id,
        any(industry_group_id)     AS industry_group_id,
        any(industry_group_name)   AS industry_group_name
    FROM {{ ref('gics_hierarchy') }}
    GROUP BY sector_name
)

SELECT
    s.equity_id,
    s.ticker,
    s.market,
    s.name,
    s.sector,
    s.industry,
    s.currency,
    s.country,
    s.market_cap,
    s.pe_ratio,
    s.fetched_at,
    g.sector_id,
    g.industry_group_id,
    g.industry_group_name
FROM snapshot AS s
LEFT JOIN gics AS g ON s.sector = g.sector_name
