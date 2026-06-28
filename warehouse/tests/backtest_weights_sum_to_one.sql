SELECT
    backtest_id,
    rebalance_date,
    sum(position_weight) AS total_weight
FROM {{ ref('mart_backtest_trades') }}
GROUP BY backtest_id, rebalance_date
HAVING abs(sum(position_weight) - 1.0) > 0.0001
