{{ config(
    materialized='table',
) }}

-- Pivot: one row per date, one column per ticker indicator.
-- FILTER aggregation selects signal for a specific ticker on a given date.
-- COALESCE to 0 handles rare data gaps for liquid instruments.
WITH pivoted AS (
    SELECT
        ts,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = 'HYG'),  0) AS hyg_return_20d,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'HYG'),  0) AS hyg_above_sma50,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = 'LQD'),  0) AS lqd_return_20d,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'LQD'),  0) AS lqd_above_sma50,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = '^TNX'), 0) AS tnx_return_20d,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = 'TLT'),  0) AS tlt_return_20d,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'VNQ'),  0) AS vnq_above_sma50,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = 'GLD'),  0) AS gld_return_20d,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = 'CL=F'), 0) AS clf_return_20d,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'TIP'),  0) AS tip_above_sma50,
        COALESCE(MAX(above_sma_20)    FILTER (WHERE ticker = 'UUP'),  0) AS uup_above_sma20,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'UUP'),  0) AS uup_above_sma50,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = 'UUP'),  0) AS uup_return_20d,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'DBB'),  0) AS dbb_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'CPER'), 0) AS cper_above_sma50,
        COALESCE(MAX(return_20d_pct)  FILTER (WHERE ticker = 'HG=F'), 0) AS hgf_return_20d,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'EEM'),  0) AS eem_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'EFA'),  0) AS efa_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'XLK'),  0) AS xlk_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'XLF'),  0) AS xlf_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'XLI'),  0) AS xli_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'XLE'),  0) AS xle_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'XLU'),  0) AS xlu_above_sma50,
        COALESCE(MAX(above_sma_50)    FILTER (WHERE ticker = 'XLV'),  0) AS xlv_above_sma50
    FROM {{ ref('int_macro_assets_daily') }}
    GROUP BY ts
),

-- credit_score computed first because rates_score depends on it.
with_credit AS (
    SELECT
        *,
        LEAST(100, GREATEST(0,
            CASE WHEN hyg_return_20d > 0  THEN 40 ELSE 0 END +
            CASE WHEN hyg_above_sma50 = 1 THEN 10 ELSE 0 END +
            CASE WHEN lqd_return_20d > 0  THEN 40 ELSE 0 END +
            CASE WHEN lqd_above_sma50 = 1 THEN 10 ELSE 0 END
        ))::double precision AS credit_score
    FROM pivoted
),

with_all_scores AS (
    SELECT
        ts,
        credit_score,
        -- rates_score: uses credit_score to disambiguate TLT rally (easing vs flight-to-safety)
        LEAST(100, GREATEST(0,
            40 +
            CASE WHEN tnx_return_20d < 0                             THEN  30 ELSE 0 END +
            CASE WHEN tnx_return_20d > 1.0                           THEN -20 ELSE 0 END +
            CASE WHEN tlt_return_20d > 2.0 AND credit_score >= 60    THEN  20 ELSE 0 END +
            CASE WHEN tlt_return_20d > 2.0 AND credit_score < 40     THEN -30 ELSE 0 END +
            CASE WHEN vnq_above_sma50 = 1                            THEN  10 ELSE 0 END
        ))::double precision AS rates_score,
        -- inflation_score: commodity spike = equity drag; disinflation = equity-friendly
        LEAST(100, GREATEST(0,
            60 +
            CASE WHEN gld_return_20d > 3.0 AND clf_return_20d > 3.0  THEN -40 ELSE 0 END +
            CASE WHEN tip_above_sma50 = 1                             THEN -20 ELSE 0 END +
            CASE WHEN gld_return_20d < 0   AND clf_return_20d < 0     THEN  30 ELSE 0 END
        ))::double precision AS inflation_score,
        -- dollar_score: strong dollar = risk-off for equities/commodities/EM
        LEAST(100, GREATEST(0,
            80 +
            CASE WHEN uup_above_sma20 = 1    THEN -40 ELSE 0 END +
            CASE WHEN uup_above_sma50 = 1    THEN -20 ELSE 0 END +
            CASE WHEN uup_return_20d > 2.0   THEN -20 ELSE 0 END
        ))::double precision AS dollar_score,
        -- growth_score: industrial metals + EM = global growth proxy
        LEAST(100, GREATEST(0,
            10 +
            CASE WHEN dbb_above_sma50  = 1  THEN 20 ELSE 0 END +
            CASE WHEN cper_above_sma50 = 1  THEN 20 ELSE 0 END +
            CASE WHEN hgf_return_20d   > 0  THEN 15 ELSE 0 END +
            CASE WHEN eem_above_sma50  = 1  THEN 20 ELSE 0 END +
            CASE WHEN efa_above_sma50  = 1  THEN 15 ELSE 0 END
        ))::double precision AS growth_score,
        -- sector_score: cyclicals leading = risk-on; defensives leading = risk-off
        LEAST(100, GREATEST(0,
            30 +
            CASE WHEN xlk_above_sma50 = 1   THEN  15 ELSE 0 END +
            CASE WHEN xlf_above_sma50 = 1   THEN  15 ELSE 0 END +
            CASE WHEN xli_above_sma50 = 1   THEN  15 ELSE 0 END +
            CASE WHEN xle_above_sma50 = 1   THEN  10 ELSE 0 END +
            CASE WHEN xlu_above_sma50 = 1   THEN -20 ELSE 0 END +
            CASE WHEN xlv_above_sma50 = 1   THEN -15 ELSE 0 END
        ))::double precision AS sector_score
    FROM with_credit
),

with_composite AS (
    SELECT
        ts,
        credit_score,
        rates_score,
        inflation_score,
        dollar_score,
        growth_score,
        sector_score,
        ROUND((
            credit_score    * 0.25 +
            growth_score    * 0.25 +
            dollar_score    * 0.15 +
            sector_score    * 0.15 +
            rates_score     * 0.10 +
            inflation_score * 0.10
        )::numeric, 2)::double precision AS composite_macro_score
    FROM with_all_scores
),

benchmark AS (
    SELECT
        ts,
        close              AS benchmark_close,
        return_pct         AS benchmark_return_pct,
        return_20d_pct     AS benchmark_return_20d_pct,
        CASE WHEN close > sma_50  THEN TRUE ELSE FALSE END AS benchmark_above_sma_50,
        CASE WHEN close > sma_200 THEN TRUE ELSE FALSE END AS benchmark_above_sma_200
    FROM {{ ref('int_market_indexes_daily') }}
    WHERE index_id = 'sp500'
)

SELECT
    c.ts,
    c.credit_score,
    c.rates_score,
    c.inflation_score,
    c.dollar_score,
    c.growth_score,
    c.sector_score,
    c.composite_macro_score,
    CASE
        WHEN c.composite_macro_score >= 80 THEN 'risk_on'
        WHEN c.composite_macro_score >= 60 THEN 'constructive'
        WHEN c.composite_macro_score >= 40 THEN 'neutral'
        WHEN c.composite_macro_score >= 20 THEN 'defensive'
        ELSE 'risk_off'
    END AS macro_regime,
    -- Legacy backward-compat: downstream models read market_regime (3-state) and risk_on_score
    CASE
        WHEN c.composite_macro_score >= 60 THEN 'risk_on'
        WHEN c.composite_macro_score >= 40 THEN 'neutral'
        ELSE 'risk_off'
    END AS market_regime,
    c.composite_macro_score AS risk_on_score,
    b.benchmark_close,
    b.benchmark_return_pct,
    b.benchmark_return_20d_pct,
    b.benchmark_above_sma_50,
    b.benchmark_above_sma_200
FROM with_composite AS c
LEFT JOIN benchmark AS b ON c.ts = b.ts
