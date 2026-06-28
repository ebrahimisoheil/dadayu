WITH expected AS (
    SELECT DISTINCT backtest_id
    FROM {{ ref('mart_backtest_strategy_variants') }}
),

signal_ids AS (
    SELECT DISTINCT backtest_id
    FROM {{ ref('mart_backtest_signals_daily') }}
),

trade_ids AS (
    SELECT DISTINCT backtest_id
    FROM {{ ref('mart_backtest_trades') }}
),

performance_ids AS (
    SELECT DISTINCT backtest_id
    FROM {{ ref('mart_backtest_performance') }}
)

SELECT e.backtest_id, 'signals' AS missing_from
FROM expected AS e
WHERE EXISTS (SELECT 1 FROM signal_ids)
  AND e.backtest_id NOT IN (SELECT backtest_id FROM signal_ids)

UNION ALL

SELECT e.backtest_id, 'trades' AS missing_from
FROM expected AS e
WHERE EXISTS (SELECT 1 FROM trade_ids)
  AND e.backtest_id NOT IN (SELECT backtest_id FROM trade_ids)

UNION ALL

SELECT e.backtest_id, 'performance' AS missing_from
FROM expected AS e
WHERE EXISTS (SELECT 1 FROM performance_ids)
  AND e.backtest_id NOT IN (SELECT backtest_id FROM performance_ids)
