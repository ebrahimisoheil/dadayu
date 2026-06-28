# Point-in-Time Index Universe — Design Spec

**Date:** 2026-06-28
**Status:** Approved for implementation

---

## Problem

The live momentum product (The Signal) claims an **800+ stock universe** —
Germany DAX-family + USA S&P 500 — picking the **top 10 per market** each month.

Reality in the repo:

1. **The German universe silently collapses.** `dadayu/ingest/equity.py::_get_germany_tickers()`
   scrapes DAX/MDAX/SDAX/TecDAX from Wikipedia via fragile column-name sniffing. When
   Wikipedia changes layout the function returns `[]` or a partial list, and
   `get_tickers()` falls back to only the 11 names in `equity_universe_extra.csv`. No error,
   no gate. US has a GitHub backup; **German has none**. DE coverage is therefore
   non-deterministic — works some months, collapses to 11 others. "Top 10 of DAX family"
   becomes "top 10 of 11."

2. **No index-membership concept at all.** Backtest eligibility is a flat
   `universe_scope = 'equity'` label in `mart_backtest_signals_daily.sql` — **every ticker
   ever ingested is eligible on every date it has prices**. This produces both look-ahead
   bias (a stock is "in the universe" before it joined the index) and survivorship bias
   (delisted/demoted names vanish instead of being tradeable in their era).

3. **Backtests are not reproducible.** The universe depends on whatever Wikipedia served
   at ingest time, so the same backtest run twice can use different universes.

The existing momentum engine (`mart_portfolio_asset_scores_daily`, `ranker_weekly`,
multi-strategy `mart_backtest_signals_daily`, `mart_product_top_lists_current`) is mature
and **out of scope** to change beyond the eligibility join.

---

## Goals

1. **Deterministic universe floor** — current DAX-family + S&P 500 membership is always
   present in the repo, surviving any scrape failure.
2. **Point-in-time membership** — every backtest date uses the universe as it actually was
   on that date (no survivorship / look-ahead bias).
3. **Self-maintaining forward** — each ingest run records membership as SCD2 so going
   forward, point-in-time history accrues automatically and exactly.
4. **Fail loud** — an asset check blocks the pipeline if active member count drops below a
   per-market floor, instead of silently shrinking to 11.
5. **Wire eligibility into the backtest** — replace the flat `universe_scope='equity'` with
   a membership-aware join.

Non-goals: changing momentum scoring/ranking math; IBKR; Telegram; India market.

---

## Architecture

### Data model: membership as dated spans

A ticker is **tradeable on date D** if any membership span covers D:

```
valid_from <= D < coalesce(valid_to, 'infinity')
```

Three layers feed one unified membership table:

| Layer | Source | Role |
|---|---|---|
| **Backfill seed** | committed CSVs reconstructed from authoritative history | point-in-time history before go-live |
| **Floor seed** | committed CSV of *current* members | guarantees coverage if scrape dies |
| **Observed SCD2** | live scrape each ingest → dbt snapshot | exact point-in-time going forward |

### 1. Backfill seeds (history)

**DE — `seed_index_membership_de.csv`**
Source: STOXX / Deutsche Börse **"Historical Index Compositions of DAX Equity Indices"**
(public, auto-updated; stable URL
`https://www.stoxx.com/document/Indices/Common/Indexguide/Historical_Index_Compositions.pdf`,
latest revision 22 Jun 2026). Covers DAX (from 1987-12-30), TecDAX (2003-03-24),
MDAX (1994), SDAX — each as an **Initial Composition** anchor list plus a
`Date of change | Date of announcement | Deletion | Addition` event table.

Reconstruction: start from the anchor composition, apply each dated deletion/addition to
open/close spans per (company, index).

**US — `seed_index_membership_us.csv`**
Source: GitHub historical S&P 500 constituents dataset (changes back to ~1996), validated
against the current Wikipedia list.

### 2. Name→ticker crosswalk — `seed_index_name_ticker_map.csv`

STOXX data is keyed by **German company name** (defunct firms included), not ticker/ISIN.
Columns: `index_name_raw, ticker, market, valid_from, valid_to` (last two optional, for
renames/reincorporations). Mapping rules:
- Only names mappable to a yfinance `.DE` (DE) / plain (US) ticker with price history are
  kept; defunct/unmappable names drop out naturally (they have no tradeable data anyway).
- Build the map for the **backtest-relevant era** first (roughly last ~15 years, where
  names map to live tickers). Pre-2010 defunct names are out of scope — they cannot be
  backtested.
- A small fixture of known renames (e.g. Daimler→Mercedes-Benz `MBG.DE`,
  `VOW3.DE` voting class) lives in this seed.

### 3. Floor seed — current members

`seed_index_membership_*` rows whose `valid_to` is null = current members, derived from the
backfill's latest state and reconciled to a current scrape at build time. This is the floor:
even with zero network access, the current universe is fully present.

### 4. Observed membership → SCD2 snapshot

- Ingest: `get_tickers()` already scrapes current DAX/MDAX/SDAX/TecDAX + S&P 500. Persist
  the raw result to `index_membership_observed` (`ticker, market, index_name, observed_at`).
- New dbt snapshot `snap_index_membership` with `strategy='check'`,
  `check_cols=['index_name']`, **`invalidate_hard_deletes=true`**. First-seen opens
  `dbt_valid_from`; drop-out closes `dbt_valid_to`. From go-live on, membership history is
  exact and free.
- This is distinct from the existing `snap_dim_equity_symbol` (which tracks *metadata*, not
  membership) — that snapshot is unchanged.

### 5. Unified membership model — `int_universe_membership_daily`

dbt model (or a date-spine macro) that UNIONs the three layers into one set of
non-overlapping spans per (ticker, market), precedence: observed SCD2 > backfill seed for
overlapping dates (live truth wins over reconstructed history). Exposes either spans or a
date×ticker eligibility grid keyed on the backtest calendar.

### 6. Wire into backtest

In `mart_backtest_signals_daily.sql`, replace the constant `'equity' AS universe_scope`
eligibility with a join to `int_universe_membership_daily` on
`ticker = ticker AND signal_date ∈ [valid_from, valid_to)`. Tickers outside their membership
span on `signal_date` are excluded from candidate ranking. The live current-rebalance path
uses active (null `valid_to`) members.

### 7. Validation gate (fail loud)

Dagster asset check on the universe asset:
- `active DE members >= 120` (DAX 40 + MDAX 50 + SDAX 70 + TecDAX overlap ≈ 160 nominal;
  120 floor allows slack)
- `active US members >= 450` (S&P 500)

Below floor → asset check **fails**, blocking downstream marts. Thresholds configurable in
the asset config, not hard-coded in SQL.

---

## Build sequence

1. Parser script `tools/parse_stoxx_compositions.py` → produces `seed_index_membership_de.csv`
   from the STOXX PDF (committed output; script committed for reproducibility/refresh).
2. US backfill seed from GitHub dataset.
3. Name→ticker crosswalk seed (backtest-era names).
4. `index_membership_observed` write in `equity.py` ingest + `snap_index_membership` snapshot.
5. `int_universe_membership_daily` model.
6. Re-point `mart_backtest_signals_daily` eligibility.
7. Dagster asset check with floor thresholds.

---

## Testing

**Unit (Python, span logic):**
- date inside / outside span; open-ended span (null `valid_to`); membership boundary
  (inclusive `valid_from`, exclusive `valid_to`)
- a ticker with multiple non-contiguous spans (left index, rejoined later)
- a ticker in two indices simultaneously (e.g. HDAX overlap) → single tradeable span via union

**Parser:**
- golden test: a known DAX change (e.g. 2009-03-23 Infineon deletion / 2009-09-21 re-addition)
  reconstructs to the expected spans from the anchor + event table.

**dbt tests:**
- no overlapping spans per (ticker, market) in `int_universe_membership_daily`
- `valid_from < valid_to` where `valid_to` not null
- active-count thresholds per market (mirrors the asset check)
- referential: every observed ticker maps to a crosswalk entry or is logged as unmapped

**Reproducibility:**
- same backtest date range + same committed seeds → byte-identical eligible-universe set
  across two runs (the core regression this whole spec exists to fix).

---

## Risks & limitations

- **Name→ticker mapping is the hard part.** German names are messy (umlauts, AG suffixes,
  share-class splits). Mitigation: scope the crosswalk to the backtest-relevant era; log and
  surface unmapped names rather than silently dropping; treat the crosswalk as a maintained
  fixture.
- **STOXX PDF format may drift.** The parser is best-effort table extraction. Mitigation:
  golden tests on known changes; the committed seed is the source of truth, the parser only
  refreshes it.
- **Pre-2010 history is non-tradeable** (defunct names, no yfinance data) and is excluded by
  design — documented in `DATA.md`.
- **MDAX/SDAX/TecDAX historical depth** is bounded by what maps to live tickers; DAX is the
  most complete. Acceptable: the live product trades current members; deep history mainly
  serves backtest honesty, where DAX-family blue/mid caps dominate the signal anyway.
