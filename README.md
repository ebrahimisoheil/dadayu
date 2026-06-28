# DADAYU AI

Local daily-only stock warehouse for portfolio ranking, sector/industry intelligence, market regime signals, and strategy backtests.

## Scope

- US and Germany equities, daily bars only
- Market indexes, daily bars only
- Portfolio scoring, sector/industry exploration, product stock lists, and backtests

The app/API layer and trade execution are intentionally out of scope for this repo reset.

## Warehouse Layout

```text
warehouse/models/
├── 01_staging/
├── 02_intermediate/
└── 03_marts/
    ├── backtests/
    ├── market/
    ├── portfolio/
    └── product/
```

Naming convention:

- `stg_<source>__<entity>_<grain>`
- `int_<domain>_<entity>_<grain>`
- `mart_<domain>_<output>_<grain>`
- snapshots stay as `snap_*`

## Seeds

- `equity_universe_extra.csv`
- `gics_hierarchy.csv`
- `index_universe.csv`

## Main Marts

- `mart_portfolio_asset_scores_daily`
- `mart_portfolio_ranker_weekly`
- `mart_sector_scores_daily`
- `mart_industry_scores_daily`
- `mart_product_top_lists_current`
- `mart_market_regime_daily`

## Backtests

- `mart_backtest_signals_daily`
- `mart_backtest_trades`
- `mart_backtest_portfolio_equity_daily`
- `mart_backtest_performance`

Default simulated variants:

- `momentum_base_daily_top40`
- `momentum_base_weekly_top40`
- `momentum_base_monthly_top40`
- `momentum_base_quarterly_top40`

## Validation

Common local checks:

```bash
cd warehouse
dbt build --select path:models/01_staging path:models/02_intermediate path:models/03_marts --exclude path:models/03_marts/backtests
dbt build --select path:models/03_marts/backtests
```

The reset keeps active ingestion and dbt models on supported daily markets only.
