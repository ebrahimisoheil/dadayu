SELECT *
FROM {{ ref('mart_backtest_production_candidates') }}
WHERE is_production_candidate
  AND (
      sharpe_ratio < 0.75
      OR max_drawdown_pct < -45
      OR total_trades < 100
      OR train_sharpe_ratio <= 0
      OR validation_sharpe_ratio <= 0
      OR test_sharpe_ratio <= 0
      OR top1_abs_contribution_share_pct > 35
      OR top5_abs_contribution_share_pct > 75
  )
