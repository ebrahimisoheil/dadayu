WITH rebalance_schedule AS (
    SELECT
        backtest_id,
        rebalance_date,
        lead(rebalance_date, 1) OVER (
            PARTITION BY backtest_id
            ORDER BY rebalance_date
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS next_rebalance_date
    FROM (
        SELECT DISTINCT
            backtest_id,
            rebalance_date
        FROM {{ ref('mart_backtest_trades') }}
    )
)

SELECT
    t.backtest_id,
    t.rebalance_date,
    t.ticker,
    t.market,
    t.entry_date,
    t.exit_date,
    s.next_rebalance_date
FROM {{ ref('mart_backtest_trades') }} AS t
INNER JOIN rebalance_schedule AS s
    ON t.backtest_id = s.backtest_id
    AND t.rebalance_date = s.rebalance_date
WHERE s.next_rebalance_date IS NOT NULL
  AND (
      t.entry_date >= s.next_rebalance_date
      OR t.exit_date > s.next_rebalance_date + INTERVAL '10 days'
  )
