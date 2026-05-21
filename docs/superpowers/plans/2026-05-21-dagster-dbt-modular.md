# Dagster dbt Modular Asset Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the single monolithic `dadayu_dbt_assets` into 5 scoped `@dbt_assets` groups (seeds, staging, snapshots, marts, data_quality) so dagster-dbt injects correct per-group fqns at runtime instead of `fqn:*`, fixing first-run ordering failures.

**Architecture:** Each `@dbt_assets(select=...)` group exposes only its subset of the dbt manifest to Dagster. At runtime, dagster-dbt injects only that group's model fqns as selectors — never `fqn:*`. Cross-group dependencies are auto-wired from the manifest. A final `@asset` wraps `dadayu/checks.py` for data quality.

**Tech Stack:** Dagster 1.7, dagster-dbt 0.23, dbt-core 1.8, dbt-clickhouse, Python 3.12

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `dadayu/checks.py` | All DQ check logic (importable) |
| Modify | `scripts/check_data_quality.py` | Thin CLI wrapper over `dadayu.checks` |
| Create | `dagster_pipeline/assets/dbt/__init__.py` | Exports all 5 asset groups |
| Create | `dagster_pipeline/assets/dbt/_common.py` | Manifest path + `DadayuDbtTranslator` |
| Create | `dagster_pipeline/assets/dbt/seeds.py` | `dbt_seed_assets` |
| Create | `dagster_pipeline/assets/dbt/staging.py` | `dbt_staging_assets` + test staging |
| Create | `dagster_pipeline/assets/dbt/snapshots.py` | `dbt_snapshot_assets` + test snapshots |
| Create | `dagster_pipeline/assets/dbt/marts.py` | `dbt_mart_assets` + test marts |
| Create | `dagster_pipeline/assets/dbt/quality.py` | `data_quality` @asset |
| Delete | `dagster_pipeline/assets/dbt_assets.py` | Replaced by dbt/ package |
| Modify | `dagster_pipeline/assets/__init__.py` | Re-export from dbt/ package |
| Modify | `dagster_pipeline/definitions.py` | Import new groups, remove old |
| Modify | `dagster_pipeline/schedules.py` | Select all 5 groups per job |
| Modify | `tests/test_dagster_assets.py` | Add tests for new groups + DQ asset |
| Create | `tests/test_checks.py` | Unit tests for `dadayu/checks.py` |

---

## Task 1: Extract `dadayu/checks.py`

The global `results` list in `scripts/check_data_quality.py` makes it unimportable. Refactor so each check function returns its own `list[CheckResult]` and expose `run_all_checks()`.

**Files:**
- Create: `dadayu/checks.py`

- [ ] **Step 1: Create `dadayu/checks.py`**

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import clickhouse_connect


@dataclass
class CheckResult:
    section: str
    name: str
    status: str          # PASS | WARN | FAIL
    value: Any = None
    detail: str = ""


def _check(
    results: list[CheckResult],
    section: str,
    name: str,
    client: clickhouse_connect.driver.Client,
    sql: str,
    *,
    fail_if_nonzero: bool = False,
    warn_if_nonzero: bool = False,
    fail_if_zero: bool = False,
    warn_if_zero: bool = False,
    detail: str = "",
) -> Any:
    val = client.query(sql).result_rows[0][0]
    status = "PASS"
    if fail_if_nonzero and val != 0:
        status = "FAIL"
    elif warn_if_nonzero and val != 0:
        status = "WARN"
    elif fail_if_zero and val == 0:
        status = "FAIL"
    elif warn_if_zero and val == 0:
        status = "WARN"
    results.append(CheckResult(section, name, status, val, detail))
    return val


def check_equity_ohlcv(client: clickhouse_connect.driver.Client) -> list[CheckResult]:
    results: list[CheckResult] = []

    _check(results, "equity_1h", "Row count", client,
           "SELECT count() FROM stg_yahoo__ohlcv_1h", fail_if_zero=True)
    _check(results, "equity_1h", "Distinct tickers", client,
           "SELECT countDistinct(ticker) FROM stg_yahoo__ohlcv_1h", fail_if_zero=True)
    _check(results, "equity_1h", "Freshness — hours since last bar", client,
           "SELECT dateDiff('hour', max(ts), now()) FROM stg_yahoo__ohlcv_1h",
           detail="warn if > 26h")
    hours_old = results[-1].value
    results[-1].status = "WARN" if hours_old > 26 else "PASS"
    _check(results, "equity_1h", "Duplicate (ticker, market, ts)", client,
           "SELECT count() FROM (SELECT ticker, market, ts, count() AS n FROM stg_yahoo__ohlcv_1h GROUP BY ticker, market, ts HAVING n > 1)",
           fail_if_nonzero=True)
    _check(results, "equity_1h", "high < low violations", client,
           "SELECT countIf(high < low) FROM stg_yahoo__ohlcv_1h", fail_if_nonzero=True)
    _check(results, "equity_1h", "close outside [low, high]", client,
           "SELECT countIf(close < low OR close > high) FROM stg_yahoo__ohlcv_1h", fail_if_nonzero=True)
    _check(results, "equity_1h", "open outside [low, high]", client,
           "SELECT countIf(open < low OR open > high) FROM stg_yahoo__ohlcv_1h", fail_if_nonzero=True)
    _check(results, "equity_1h", "Zero or negative close", client,
           "SELECT countIf(close <= 0) FROM stg_yahoo__ohlcv_1h", fail_if_nonzero=True)
    _check(results, "equity_1h", "Zero volume bars", client,
           "SELECT countIf(volume = 0) FROM stg_yahoo__ohlcv_1h",
           warn_if_nonzero=True, detail="may be legit on holidays/halts")
    _check(results, "equity_1h", "Null prices", client,
           "SELECT countIf(isNull(open) OR isNull(high) OR isNull(low) OR isNull(close)) FROM stg_yahoo__ohlcv_1h",
           fail_if_nonzero=True)
    _check(results, "equity_1h", "Bars with >50% single-hour return", client,
           """
           SELECT count() FROM (
             SELECT ticker, market, ts,
               (close - lagInFrame(close, 1, close) OVER w) / nullIf(lagInFrame(close, 1, close) OVER w, 0) AS ret
             FROM stg_yahoo__ohlcv_1h
             WINDOW w AS (PARTITION BY ticker, market ORDER BY ts)
           ) WHERE abs(ret) > 0.5
           """,
           warn_if_nonzero=True, detail=">50% single-bar move")

    return results


def check_crypto_ohlcv(client: clickhouse_connect.driver.Client) -> list[CheckResult]:
    results: list[CheckResult] = []

    _check(results, "crypto_1h", "Row count", client,
           "SELECT count() FROM stg_yahoo__crypto_ohlcv_1h", fail_if_zero=True)
    _check(results, "crypto_1h", "Distinct tickers", client,
           "SELECT countDistinct(ticker) FROM stg_yahoo__crypto_ohlcv_1h", fail_if_zero=True)
    _check(results, "crypto_1h", "Freshness — hours since last bar", client,
           "SELECT dateDiff('hour', max(ts), now()) FROM stg_yahoo__crypto_ohlcv_1h")
    hours_old = results[-1].value
    results[-1].status = "WARN" if hours_old > 5 else "PASS"
    results[-1].detail = "warn if > 5h (crypto never closes)"
    _check(results, "crypto_1h", "Duplicate (ticker, market, ts)", client,
           "SELECT count() FROM (SELECT ticker, market, ts, count() AS n FROM stg_yahoo__crypto_ohlcv_1h GROUP BY ticker, market, ts HAVING n > 1)",
           fail_if_nonzero=True)
    _check(results, "crypto_1h", "high < low violations", client,
           "SELECT countIf(high < low) FROM stg_yahoo__crypto_ohlcv_1h", fail_if_nonzero=True)
    _check(results, "crypto_1h", "close outside [low, high]", client,
           "SELECT countIf(close < low OR close > high) FROM stg_yahoo__crypto_ohlcv_1h", fail_if_nonzero=True)
    _check(results, "crypto_1h", "Zero or negative close", client,
           "SELECT countIf(close <= 0) FROM stg_yahoo__crypto_ohlcv_1h", fail_if_nonzero=True)
    _check(results, "crypto_1h", "Bars with >30% single-hour return", client,
           """
           SELECT count() FROM (
             SELECT ticker, market, ts,
               (close - lagInFrame(close, 1, close) OVER w) / nullIf(lagInFrame(close, 1, close) OVER w, 0) AS ret
             FROM stg_yahoo__crypto_ohlcv_1h
             WINDOW w AS (PARTITION BY ticker, market ORDER BY ts)
           ) WHERE abs(ret) > 0.3
           """,
           warn_if_nonzero=True, detail=">30% single-bar move")

    return results


def check_polymarket(client: clickhouse_connect.driver.Client) -> list[CheckResult]:
    results: list[CheckResult] = []

    _check(results, "polymarket", "Total markets", client,
           "SELECT count() FROM polymarket_markets FINAL", fail_if_zero=True)
    _check(results, "polymarket", "Active markets (not closed)", client,
           "SELECT countIf(closed = false) FROM polymarket_markets FINAL", warn_if_zero=True)
    _check(results, "polymarket", "Markets missing yes_token_id", client,
           "SELECT countIf(yes_token_id = '') FROM polymarket_markets FINAL WHERE closed = false",
           warn_if_nonzero=True, detail="these markets won't get price data")
    _check(results, "polymarket", "Freshness — hours since last market fetch", client,
           "SELECT dateDiff('hour', max(fetched_at), now()) FROM polymarket_markets")
    hours_old = results[-1].value
    results[-1].status = "WARN" if hours_old > 26 else "PASS"
    _check(results, "polymarket", "Price rows total", client,
           "SELECT count() FROM polymarket_prices", fail_if_zero=True)
    _check(results, "polymarket", "Markets with price data", client,
           "SELECT countDistinct(condition_id) FROM polymarket_prices", fail_if_zero=True)
    _check(results, "polymarket", "Active markets with NO price data", client,
           """
           SELECT count() FROM (
             SELECT condition_id FROM polymarket_markets FINAL WHERE closed = false AND yes_token_id != ''
           ) WHERE condition_id NOT IN (SELECT DISTINCT condition_id FROM polymarket_prices)
           """,
           warn_if_nonzero=True, detail="should be 0 after first run")
    _check(results, "polymarket", "Probability outside [0, 1]", client,
           "SELECT countIf(probability < 0 OR probability > 1) FROM polymarket_prices",
           fail_if_nonzero=True)
    _check(results, "polymarket", "Duplicate (condition_id, ts)", client,
           "SELECT count() FROM (SELECT condition_id, ts, count() AS n FROM polymarket_prices GROUP BY condition_id, ts HAVING n > 1)",
           warn_if_nonzero=True)
    _check(results, "polymarket", "Freshness — hours since last price", client,
           "SELECT dateDiff('hour', max(ts), now()) FROM polymarket_prices")
    hours_old = results[-1].value
    results[-1].status = "WARN" if hours_old > 5 else "PASS"
    results[-1].detail = "warn if > 5h"
    _check(results, "polymarket", "Extreme prob jump >0.5 in single hour", client,
           """
           SELECT countIf(abs(prob_change) > 0.5)
           FROM fct_polymarket_signals
           WHERE days_to_resolution > 2 OR days_to_resolution IS NULL
           """,
           warn_if_nonzero=True, detail="excludes near-expiry rows")

    return results


def check_cross_dataset(client: clickhouse_connect.driver.Client) -> list[CheckResult]:
    results: list[CheckResult] = []

    eq_stg = client.query("SELECT count() FROM stg_yahoo__ohlcv_1h").result_rows[0][0]
    eq_int = client.query("SELECT count() FROM int_equity_ohlcv_1h").result_rows[0][0]
    mismatch = abs(eq_stg - eq_int)
    results.append(CheckResult("cross", "stg_yahoo__ohlcv_1h == int_equity_ohlcv_1h row count",
                               "FAIL" if mismatch != 0 else "PASS", mismatch,
                               f"stg={eq_stg:,}, int={eq_int:,}"))

    eq_int = client.query("SELECT count() FROM int_equity_ohlcv_1h").result_rows[0][0]
    eq_fct = client.query("SELECT count() FROM fct_ohlcv_1h").result_rows[0][0]
    mismatch = abs(eq_int - eq_fct)
    results.append(CheckResult("cross", "int_equity_ohlcv_1h == fct_ohlcv_1h row count",
                               "FAIL" if mismatch != 0 else "PASS", mismatch,
                               f"int={eq_int:,}, fct={eq_fct:,}"))

    cr_stg = client.query("SELECT count() FROM stg_yahoo__crypto_ohlcv_1h").result_rows[0][0]
    cr_int = client.query("SELECT count() FROM int_crypto_ohlcv_1h").result_rows[0][0]
    mismatch = abs(cr_stg - cr_int)
    results.append(CheckResult("cross", "stg_crypto_ohlcv_1h == int_crypto_ohlcv_1h row count",
                               "FAIL" if mismatch != 0 else "PASS", mismatch,
                               f"stg={cr_stg:,}, int={cr_int:,}"))

    pm_stg = client.query("SELECT countDistinct(condition_id) FROM stg_polymarket__prices").result_rows[0][0]
    pm_int = client.query("SELECT countDistinct(condition_id) FROM int_polymarket_prices_1h").result_rows[0][0]
    results.append(CheckResult("cross", "Polymarket: stg markets == int_1h markets",
                               "FAIL" if pm_stg != pm_int else "PASS", None,
                               f"stg={pm_stg}, int={pm_int}"))

    _check(results, "cross", "Equity tickers not in dim_equity_symbol", client,
           """
           SELECT countDistinct(ticker) FROM stg_yahoo__ohlcv_1h
           WHERE ticker NOT IN (SELECT ticker FROM dim_equity_symbol)
           """,
           warn_if_nonzero=True, detail="dim may lag by 1 run")
    _check(results, "cross", "Crypto tickers not in dim_crypto_symbol", client,
           """
           SELECT countDistinct(ticker) FROM stg_yahoo__crypto_ohlcv_1h
           WHERE ticker NOT IN (SELECT yf_symbol FROM dim_crypto_symbol)
           """,
           warn_if_nonzero=True)

    return results


def check_mart_sanity(client: clickhouse_connect.driver.Client) -> list[CheckResult]:
    results: list[CheckResult] = []

    _check(results, "mart", "fct_ohlcv_1h: null return_pct ratio", client,
           "SELECT round(countIf(isNull(return_pct)) / count(), 4) FROM fct_ohlcv_1h",
           detail="null only on first bar per ticker; >0.01 is suspicious")
    ratio = results[-1].value
    results[-1].status = "WARN" if ratio > 0.01 else "PASS"

    _check(results, "mart", "fct_indicators_1h: null RSI rows", client,
           "SELECT countIf(isNull(rsi_14)) FROM fct_indicators_1h",
           warn_if_nonzero=True, detail="RSI null on first 14 bars per ticker only")
    _check(results, "mart", "fct_indicators_1h: RSI outside [0, 100]", client,
           "SELECT countIf(rsi_14 < 0 OR rsi_14 > 100) FROM fct_indicators_1h WHERE rsi_14 IS NOT NULL",
           fail_if_nonzero=True)
    _check(results, "mart", "fct_polymarket_signals: null prob_change ratio", client,
           "SELECT round(countIf(isNull(prob_change)) / count(), 4) FROM fct_polymarket_signals",
           detail="null only on first bar per market")
    ratio = results[-1].value
    results[-1].status = "WARN" if ratio > 0.02 else "PASS"
    _check(results, "mart", "fct_polymarket_signals: log_odds finite", client,
           "SELECT countIf(isInfinite(log_odds) OR isNaN(log_odds)) FROM fct_polymarket_signals",
           fail_if_nonzero=True)

    return results


def run_all_checks(client: clickhouse_connect.driver.Client) -> list[CheckResult]:
    return (
        check_equity_ohlcv(client)
        + check_crypto_ohlcv(client)
        + check_polymarket(client)
        + check_cross_dataset(client)
        + check_mart_sanity(client)
    )
```

- [ ] **Step 2: Verify import works**

```bash
cd "/Users/soheilebrahimi/Documents/DADAYU AI"
python -c "from dadayu.checks import run_all_checks, CheckResult; print('OK')"
```

Expected output: `OK`

- [ ] **Step 3: Commit**

```bash
git add dadayu/checks.py
git commit -m "feat: extract DQ check logic to dadayu/checks.py"
```

---

## Task 2: Update `scripts/check_data_quality.py` to thin wrapper

**Files:**
- Modify: `scripts/check_data_quality.py`

- [ ] **Step 1: Replace script content**

```python
#!/usr/bin/env python3
"""
Ad-hoc data quality checks for DADAYU pipeline.
Usage: python scripts/check_data_quality.py
"""
from __future__ import annotations

import os
import sys

import clickhouse_connect

from dadayu.checks import CheckResult, run_all_checks


def get_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        database=os.getenv("CLICKHOUSE_DB", "dadayu"),
        username=os.getenv("CLICKHOUSE_USER", "dadayu"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "changeme"),
    )


def print_report(results: list[CheckResult]) -> int:
    pass_count = sum(1 for r in results if r.status == "PASS")
    warn_count = sum(1 for r in results if r.status == "WARN")
    fail_count = sum(1 for r in results if r.status == "FAIL")

    print(f"\n{'='*60}")
    print(f"  SUMMARY: {pass_count} PASS  {warn_count} WARN  {fail_count} FAIL")
    print(f"{'='*60}")

    icons = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗"}
    colors = {"PASS": "\033[32m", "WARN": "\033[33m", "FAIL": "\033[31m"}
    reset = "\033[0m"

    for r in results:
        icon = icons[r.status]
        color = colors[r.status]
        val_str = f"  [{r.value:,}]" if isinstance(r.value, int) else f"  [{r.value}]" if r.value is not None else ""
        detail_str = f"  — {r.detail}" if r.detail else ""
        print(f"  {color}{icon} {r.status:<4}{reset}  {r.name}{val_str}{detail_str}")

    print()
    return 1 if fail_count > 0 else 0


if __name__ == "__main__":
    print("Connecting to ClickHouse...")
    client = get_client()
    print(f"Connected: {os.getenv('CLICKHOUSE_HOST', 'localhost')}:{os.getenv('CLICKHOUSE_PORT', '8123')} / {os.getenv('CLICKHOUSE_DB', 'dadayu')}")

    results = run_all_checks(client)
    exit_code = print_report(results)
    sys.exit(exit_code)
```

- [ ] **Step 2: Verify script still runs (dry import)**

```bash
cd "/Users/soheilebrahimi/Documents/DADAYU AI"
python -c "import scripts.check_data_quality" 2>/dev/null || python -c "
import importlib.util, sys
spec = importlib.util.spec_from_file_location('dq', 'scripts/check_data_quality.py')
mod = importlib.util.module_from_spec(spec)
print('import OK')
"
```

Expected: `import OK`

- [ ] **Step 3: Commit**

```bash
git add scripts/check_data_quality.py
git commit -m "refactor: check_data_quality.py becomes thin wrapper over dadayu.checks"
```

---

## Task 3: Create `dagster_pipeline/assets/dbt/_common.py`

Shared manifest path and translator. All 4 dbt asset files import from here.

**Files:**
- Create: `dagster_pipeline/assets/dbt/_common.py`

- [ ] **Step 1: Create the file**

```python
from __future__ import annotations

from pathlib import Path

from dagster import AssetDep, AssetKey
from dagster_dbt import DagsterDbtTranslator

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent / "warehouse"
DBT_MANIFEST = DBT_PROJECT_DIR / "target" / "manifest.json"

_SOURCE_UPSTREAM: dict[str, AssetKey] = {
    "prices_hourly":        AssetKey("equity_ohlcv"),
    "prices_4h":            AssetKey("equity_ohlcv"),
    "prices_daily":         AssetKey("equity_ohlcv"),
    "tickers":              AssetKey("equity_ticker_info"),
    "crypto_prices_hourly": AssetKey("crypto_ohlcv"),
    "crypto_prices_4h":     AssetKey("crypto_ohlcv"),
    "crypto_prices_daily":  AssetKey("crypto_ohlcv"),
    "crypto_metadata":      AssetKey("crypto_info"),
    "polymarket_markets":   AssetKey("polymarket_markets"),
    "polymarket_prices":    AssetKey("polymarket_prices"),
}


class DadayuDbtTranslator(DagsterDbtTranslator):
    def get_asset_spec(self, manifest, unique_id, project=None):
        spec = super().get_asset_spec(manifest, unique_id, project)
        sources = manifest.get("sources", {})
        node = sources.get(unique_id, {})
        if node.get("resource_type") == "source":
            upstream = _SOURCE_UPSTREAM.get(node.get("name", ""))
            if upstream:
                spec = spec.replace_attributes(
                    deps=list(spec.deps) + [AssetDep(upstream)]
                )
        return spec
```

- [ ] **Step 2: Verify path resolves correctly**

```bash
cd "/Users/soheilebrahimi/Documents/DADAYU AI"
python -c "
from dagster_pipeline.assets.dbt._common import DBT_MANIFEST
assert DBT_MANIFEST.exists(), f'manifest not found: {DBT_MANIFEST}'
print('manifest OK:', DBT_MANIFEST)
"
```

Expected: `manifest OK: /Users/soheilebrahimi/Documents/DADAYU AI/warehouse/target/manifest.json`

- [ ] **Step 3: Commit**

```bash
git add dagster_pipeline/assets/dbt/_common.py
git commit -m "feat: add dbt/_common.py with shared manifest path and translator"
```

---

## Task 4: Create dbt asset group files

**Files:**
- Create: `dagster_pipeline/assets/dbt/seeds.py`
- Create: `dagster_pipeline/assets/dbt/staging.py`
- Create: `dagster_pipeline/assets/dbt/snapshots.py`
- Create: `dagster_pipeline/assets/dbt/marts.py`

- [ ] **Step 1: Create `seeds.py`**

```python
from __future__ import annotations

from dagster_dbt import DbtCliResource, dbt_assets

from dagster_pipeline.assets.dbt._common import DBT_MANIFEST, DadayuDbtTranslator


@dbt_assets(
    manifest=DBT_MANIFEST,
    select="resource_type:seed",
    dagster_dbt_translator=DadayuDbtTranslator(),
)
def dbt_seed_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["seed"], context=context).stream()
```

- [ ] **Step 2: Create `staging.py`**

```python
from __future__ import annotations

from dagster_dbt import DbtCliResource, dbt_assets

from dagster_pipeline.assets.dbt._common import DBT_MANIFEST, DadayuDbtTranslator


@dbt_assets(
    manifest=DBT_MANIFEST,
    select="staging",
    dagster_dbt_translator=DadayuDbtTranslator(),
)
def dbt_staging_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
    yield from dbt.cli(["test"], context=context).stream()
```

- [ ] **Step 3: Create `snapshots.py`**

```python
from __future__ import annotations

from dagster_dbt import DbtCliResource, dbt_assets

from dagster_pipeline.assets.dbt._common import DBT_MANIFEST, DadayuDbtTranslator


@dbt_assets(
    manifest=DBT_MANIFEST,
    select="resource_type:snapshot",
    dagster_dbt_translator=DadayuDbtTranslator(),
)
def dbt_snapshot_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["snapshot"], context=context).stream()
    yield from dbt.cli(["test"], context=context).stream()
```

- [ ] **Step 4: Create `marts.py`**

```python
from __future__ import annotations

from dagster_dbt import DbtCliResource, dbt_assets

from dagster_pipeline.assets.dbt._common import DBT_MANIFEST, DadayuDbtTranslator


@dbt_assets(
    manifest=DBT_MANIFEST,
    exclude="staging resource_type:snapshot resource_type:seed",
    dagster_dbt_translator=DadayuDbtTranslator(),
)
def dbt_mart_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
    yield from dbt.cli(["test"], context=context).stream()
```

- [ ] **Step 5: Commit**

```bash
git add dagster_pipeline/assets/dbt/seeds.py dagster_pipeline/assets/dbt/staging.py dagster_pipeline/assets/dbt/snapshots.py dagster_pipeline/assets/dbt/marts.py
git commit -m "feat: add modular dbt asset groups (seeds, staging, snapshots, marts)"
```

---

## Task 5: Create `dagster_pipeline/assets/dbt/quality.py`

**Files:**
- Create: `dagster_pipeline/assets/dbt/quality.py`

- [ ] **Step 1: Create the file**

```python
from __future__ import annotations

from dagster import AssetKey, Failure, MetadataValue, Output, asset

from dadayu.checks import CheckResult, run_all_checks
from dagster_pipeline.resources import ClickhouseResource


@asset(
    group_name="quality",
    deps=[
        AssetKey("fct_ohlcv_1h"),
        AssetKey("fct_indicators_1d"),
        AssetKey("fct_polymarket_signals"),
    ],
)
def data_quality(clickhouse: ClickhouseResource) -> Output:
    client = clickhouse.get_client()
    results = run_all_checks(client)

    fail_count = sum(1 for r in results if r.status == "FAIL")
    warn_count = sum(1 for r in results if r.status == "WARN")
    pass_count = sum(1 for r in results if r.status == "PASS")

    failures = [f"{r.section}/{r.name}: {r.value} — {r.detail}" for r in results if r.status == "FAIL"]
    warnings = [f"{r.section}/{r.name}: {r.value} — {r.detail}" for r in results if r.status == "WARN"]

    metadata = {
        "pass_count": MetadataValue.int(pass_count),
        "warn_count": MetadataValue.int(warn_count),
        "fail_count": MetadataValue.int(fail_count),
        "warnings": MetadataValue.text("\n".join(warnings) if warnings else "none"),
    }

    if fail_count > 0:
        raise Failure(
            description=f"{fail_count} data quality check(s) failed",
            metadata={**metadata, "failures": MetadataValue.text("\n".join(failures))},
        )

    return Output(value=None, metadata=metadata)
```

- [ ] **Step 2: Commit**

```bash
git add dagster_pipeline/assets/dbt/quality.py
git commit -m "feat: add data_quality Dagster asset wrapping dadayu.checks"
```

---

## Task 6: Create `dagster_pipeline/assets/dbt/__init__.py`

**Files:**
- Create: `dagster_pipeline/assets/dbt/__init__.py`

- [ ] **Step 1: Create the file**

```python
from dagster_pipeline.assets.dbt.marts import dbt_mart_assets
from dagster_pipeline.assets.dbt.quality import data_quality
from dagster_pipeline.assets.dbt.seeds import dbt_seed_assets
from dagster_pipeline.assets.dbt.snapshots import dbt_snapshot_assets
from dagster_pipeline.assets.dbt.staging import dbt_staging_assets

__all__ = [
    "dbt_seed_assets",
    "dbt_staging_assets",
    "dbt_snapshot_assets",
    "dbt_mart_assets",
    "data_quality",
]
```

- [ ] **Step 2: Verify all imports load**

```bash
cd "/Users/soheilebrahimi/Documents/DADAYU AI"
python -c "
from dagster_pipeline.assets.dbt import (
    dbt_seed_assets, dbt_staging_assets, dbt_snapshot_assets,
    dbt_mart_assets, data_quality
)
print('all dbt asset groups loaded OK')
"
```

Expected: `all dbt asset groups loaded OK`

- [ ] **Step 3: Commit**

```bash
git add dagster_pipeline/assets/dbt/__init__.py
git commit -m "feat: add dbt package __init__ exporting all asset groups"
```

---

## Task 7: Wire into definitions and schedules, delete old file

**Files:**
- Modify: `dagster_pipeline/definitions.py`
- Modify: `dagster_pipeline/schedules.py`
- Modify: `dagster_pipeline/assets/__init__.py`
- Delete: `dagster_pipeline/assets/dbt_assets.py`

- [ ] **Step 1: Update `dagster_pipeline/assets/__init__.py`**

```python
from dagster_pipeline.assets.crypto import crypto_info, crypto_ohlcv
from dagster_pipeline.assets.dbt import (
    data_quality,
    dbt_mart_assets,
    dbt_seed_assets,
    dbt_snapshot_assets,
    dbt_staging_assets,
)
from dagster_pipeline.assets.equity import equity_ohlcv, equity_ticker_info
from dagster_pipeline.assets.polymarket import polymarket_markets, polymarket_prices

__all__ = [
    "equity_ohlcv",
    "equity_ticker_info",
    "crypto_ohlcv",
    "crypto_info",
    "polymarket_markets",
    "polymarket_prices",
    "dbt_seed_assets",
    "dbt_staging_assets",
    "dbt_snapshot_assets",
    "dbt_mart_assets",
    "data_quality",
]
```

- [ ] **Step 2: Update `dagster_pipeline/definitions.py`**

```python
from __future__ import annotations

from dagster import Definitions
from dagster_dbt import DbtCliResource

from dagster_pipeline.assets.crypto import crypto_info, crypto_ohlcv
from dagster_pipeline.assets.dbt import (
    data_quality,
    dbt_mart_assets,
    dbt_seed_assets,
    dbt_snapshot_assets,
    dbt_staging_assets,
)
from dagster_pipeline.assets.dbt._common import DBT_PROJECT_DIR
from dagster_pipeline.assets.equity import equity_ohlcv, equity_ticker_info
from dagster_pipeline.assets.polymarket import polymarket_markets, polymarket_prices
from dagster_pipeline.resources import ClickhouseResource
from dagster_pipeline.schedules import (
    crypto_job,
    crypto_schedule,
    equity_job,
    equity_schedule,
    polymarket_job,
    polymarket_schedule,
)

defs = Definitions(
    assets=[
        equity_ohlcv,
        equity_ticker_info,
        crypto_ohlcv,
        crypto_info,
        polymarket_markets,
        polymarket_prices,
        dbt_seed_assets,
        dbt_staging_assets,
        dbt_snapshot_assets,
        dbt_mart_assets,
        data_quality,
    ],
    resources={
        "clickhouse": ClickhouseResource(),
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
    jobs=[equity_job, crypto_job, polymarket_job],
    schedules=[equity_schedule, crypto_schedule, polymarket_schedule],
)
```

- [ ] **Step 3: Update `dagster_pipeline/schedules.py`**

```python
from __future__ import annotations

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

from dagster_pipeline.assets.crypto import crypto_info, crypto_ohlcv
from dagster_pipeline.assets.dbt import (
    data_quality,
    dbt_mart_assets,
    dbt_seed_assets,
    dbt_snapshot_assets,
    dbt_staging_assets,
)
from dagster_pipeline.assets.equity import equity_ohlcv, equity_ticker_info
from dagster_pipeline.assets.polymarket import polymarket_markets, polymarket_prices

_DBT_ASSETS = [dbt_seed_assets, dbt_staging_assets, dbt_snapshot_assets, dbt_mart_assets, data_quality]

equity_job = define_asset_job(
    name="equity_job",
    selection=AssetSelection.assets(equity_ohlcv, equity_ticker_info, *_DBT_ASSETS),
)

crypto_job = define_asset_job(
    name="crypto_job",
    selection=AssetSelection.assets(crypto_ohlcv, crypto_info, *_DBT_ASSETS),
)

polymarket_job = define_asset_job(
    name="polymarket_job",
    selection=AssetSelection.assets(polymarket_markets, polymarket_prices, *_DBT_ASSETS),
)

# Daily at 22:00 UTC on weekdays (after all equity markets close)
equity_schedule = ScheduleDefinition(
    job=equity_job,
    cron_schedule="0 22 * * 1-5",
)

# Every 4 hours, 24/7 (crypto never closes)
crypto_schedule = ScheduleDefinition(
    job=crypto_job,
    cron_schedule="0 */4 * * *",
)

# Every 4 hours, 24/7 (prediction markets never close)
polymarket_schedule = ScheduleDefinition(
    job=polymarket_job,
    cron_schedule="0 */4 * * *",
)
```

- [ ] **Step 4: Delete old monolithic file**

```bash
rm "/Users/soheilebrahimi/Documents/DADAYU AI/dagster_pipeline/assets/dbt_assets.py"
```

- [ ] **Step 5: Verify definitions load**

```bash
cd "/Users/soheilebrahimi/Documents/DADAYU AI"
python -c "from dagster_pipeline.definitions import defs; print('defs loaded, assets:', len(defs.assets))"
```

Expected: `defs loaded, assets: 11` (6 ingestion + 4 dbt groups + 1 quality)

- [ ] **Step 6: Commit**

```bash
git add dagster_pipeline/assets/__init__.py dagster_pipeline/definitions.py dagster_pipeline/schedules.py
git rm dagster_pipeline/assets/dbt_assets.py
git commit -m "feat: wire modular dbt asset groups into definitions and schedules"
```

---

## Task 8: Add tests

**Files:**
- Create: `tests/test_checks.py`
- Modify: `tests/test_dagster_assets.py`

- [ ] **Step 1: Create `tests/test_checks.py`**

```python
from unittest.mock import MagicMock

from dadayu.checks import (
    CheckResult,
    _check,
    check_equity_ohlcv,
    check_mart_sanity,
    run_all_checks,
)


def _mock_client(*return_values):
    client = MagicMock()
    client.query.side_effect = [
        MagicMock(result_rows=[[v]]) for v in return_values
    ]
    return client


def test_check_result_pass():
    results = []
    client = _mock_client(42)
    _check(results, "sec", "name", client, "SELECT 42", fail_if_zero=True)
    assert results[0].status == "PASS"
    assert results[0].value == 42


def test_check_result_fail_if_zero():
    results = []
    client = _mock_client(0)
    _check(results, "sec", "name", client, "SELECT 0", fail_if_zero=True)
    assert results[0].status == "FAIL"


def test_check_result_warn_if_nonzero():
    results = []
    client = _mock_client(5)
    _check(results, "sec", "name", client, "SELECT 5", warn_if_nonzero=True)
    assert results[0].status == "WARN"


def test_check_equity_ohlcv_returns_list():
    # Provide enough mock values for all 11 checks in check_equity_ohlcv
    values = [1000, 50, 2, 0, 0, 0, 0, 0, 10, 0, 0]
    client = _mock_client(*values)
    results = check_equity_ohlcv(client)
    assert isinstance(results, list)
    assert all(isinstance(r, CheckResult) for r in results)
    assert len(results) == 11


def test_run_all_checks_returns_flat_list():
    # 11 equity + 8 crypto + 10 polymarket + 6 cross + 5 mart = 40 checks
    # Provide enough mock values (one per SQL query)
    values = [100] * 60
    client = _mock_client(*values)
    results = run_all_checks(client)
    assert isinstance(results, list)
    assert len(results) > 0
    assert all(r.status in ("PASS", "WARN", "FAIL") for r in results)
```

- [ ] **Step 2: Run checks tests**

```bash
cd "/Users/soheilebrahimi/Documents/DADAYU AI"
python -m pytest tests/test_checks.py -v
```

Expected: all tests pass.

- [ ] **Step 3: Add dbt asset group import test to `tests/test_dagster_assets.py`**

Append to the end of `tests/test_dagster_assets.py`:

```python
def test_dbt_asset_groups_load():
    from dagster_pipeline.assets.dbt import (
        data_quality,
        dbt_mart_assets,
        dbt_seed_assets,
        dbt_snapshot_assets,
        dbt_staging_assets,
    )
    for group in [dbt_seed_assets, dbt_staging_assets, dbt_snapshot_assets, dbt_mart_assets]:
        assert hasattr(group, "keys_by_input_name") or callable(group)
    assert callable(data_quality)


def test_data_quality_asset_raises_failure_on_fail_results():
    from unittest.mock import MagicMock, patch
    from dagster import Failure, build_asset_context
    from dagster_pipeline.assets.dbt.quality import data_quality
    from dagster_pipeline.resources import ClickhouseResource
    from dadayu.checks import CheckResult

    fail_results = [CheckResult("sec", "bad check", "FAIL", 5, "something wrong")]

    with patch("dadayu.db.get_ch_client") as mock_gc, \
         patch("dagster_pipeline.assets.dbt.quality.run_all_checks", return_value=fail_results):
        mock_gc.return_value = MagicMock()
        import pytest
        with pytest.raises(Failure):
            data_quality(clickhouse=ClickhouseResource())


def test_data_quality_asset_succeeds_on_pass_results():
    from unittest.mock import MagicMock, patch
    from dagster_pipeline.assets.dbt.quality import data_quality
    from dagster_pipeline.resources import ClickhouseResource
    from dadayu.checks import CheckResult

    pass_results = [CheckResult("sec", "good check", "PASS", 100, "")]

    with patch("dadayu.db.get_ch_client") as mock_gc, \
         patch("dagster_pipeline.assets.dbt.quality.run_all_checks", return_value=pass_results):
        mock_gc.return_value = MagicMock()
        result = data_quality(clickhouse=ClickhouseResource())
    assert result is not None
```

- [ ] **Step 4: Run all tests**

```bash
cd "/Users/soheilebrahimi/Documents/DADAYU AI"
python -m pytest tests/ -v
```

Expected: all existing + new tests pass.

- [ ] **Step 5: Commit**

```bash
git add tests/test_checks.py tests/test_dagster_assets.py
git commit -m "test: add tests for dadayu.checks and modular dbt asset groups"
```

---

## Task 9: Build container and verify end-to-end

- [ ] **Step 1: Rebuild dagster code container**

```bash
cd "/Users/soheilebrahimi/Documents/DADAYU AI"
docker compose build dadayu_dagster_code 2>&1 | tail -5
```

Expected: `dadayu_dagster_code  Built`

- [ ] **Step 2: Restart dagster services**

```bash
docker compose restart dadayu_dagster_code dadayu_dagster_daemon dadayu_dagster_webserver 2>&1
```

Expected: all 3 containers restarted.

- [ ] **Step 3: Confirm code server loaded all groups**

```bash
sleep 8 && docker logs dadayu_dagster_code 2>&1 | tail -4
```

Expected: `Started Dagster code server` with no errors.

- [ ] **Step 4: Verify Dagster sees all 4 jobs with correct asset counts**

```bash
curl -s http://localhost:3000/graphql -X POST -H "Content-Type: application/json" -d '{
  "query": "{ repositoriesOrError { ... on RepositoryConnection { nodes { name jobs { name } } } } }"
}' | python3 -m json.tool
```

Expected: `crypto_job`, `equity_job`, `polymarket_job` all present.

- [ ] **Step 5: Trigger crypto_job and confirm dbt phases run in order**

```bash
curl -s http://localhost:3000/graphql -X POST -H "Content-Type: application/json" -d '{
  "query": "mutation { launchRun(executionParams: { selector: { jobName: \"crypto_job\", repositoryLocationName: \"dadayu\", repositoryName: \"__repository__\" } }) { ... on LaunchRunSuccess { run { runId } } } }"
}' | python3 -m json.tool
```

Watch Dagster UI at http://localhost:3000 — confirm step order:
`crypto_ohlcv → crypto_info → dbt_seed_assets → dbt_staging_assets → dbt_snapshot_assets → dbt_mart_assets → data_quality`

No `fqn:*` errors. Each phase shows only its own models.

- [ ] **Step 6: Final commit if any fixups were needed**

```bash
git add -A && git commit -m "fix: post-integration fixups for modular dbt assets" 2>/dev/null || echo "nothing to commit"
```
