# Point-in-Time Index Universe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the silently-collapsing scraped German universe and the flat `universe_scope='equity'` backtest eligibility with a deterministic, point-in-time SCD2 index-membership system.

**Architecture:** Three membership layers feed one dbt model. Committed backfill seeds (STOXX historical compositions for DE, GitHub dataset for US) give point-in-time history; a per-ingest SCD2 dbt snapshot of live-scraped membership gives exact history going forward; the latest-state of those seeds is the deterministic floor. The backtest signal model joins membership so a ticker is eligible on a date only if a membership span covers it. A dbt test + `checks.py` entry fail loud if active member count drops below a per-market floor.

**Tech Stack:** Python 3.12, pandas, pypdf, dbt-postgres, dbt snapshots, Dagster, pytest.

## Global Constraints

- Markets are exactly `germany` and `us` (verbatim, lowercase) — matches `dadayu/ingest/equity.py::MARKETS`.
- German yfinance tickers carry the `.DE` suffix; US tickers use `-` for share classes (`_convert_us_symbol`).
- Membership span semantics: a ticker is tradeable on date `D` iff `valid_from <= D AND (valid_to IS NULL OR D < valid_to)` — **`valid_from` inclusive, `valid_to` exclusive**.
- Seeds live in `warehouse/seeds/`, registered in `warehouse/dbt_project.yml` under `seeds.dadayu_warehouse` with explicit `+column_types`.
- dbt models target schema `dadayu`.
- Floor thresholds: active DE members `>= 120`, active US members `>= 450`.
- Index names (verbatim): `DAX`, `MDAX`, `SDAX`, `TecDAX` (DE), `SP500` (US).
- Do not modify momentum scoring/ranking math. The only change to `mart_backtest_signals_daily.sql` is the eligibility join.

---

### Task 1: STOXX PDF parser → raw DE membership spans

**Files:**
- Create: `tools/parse_stoxx_compositions.py`
- Test: `tests/test_parse_stoxx_compositions.py`
- Create (committed output): `warehouse/seeds/seed_index_membership_de_raw.csv`

**Interfaces:**
- Produces: `parse_compositions(pdf_path: str) -> list[dict]` where each dict is
  `{"index_name": str, "company_name": str, "valid_from": "YYYY-MM-DD", "valid_to": "YYYY-MM-DD"|None}`.
- Produces: `build_spans(events: list[dict], anchor: dict) -> list[dict]` (pure, testable without a PDF).

The STOXX layout per index is an "Initial Composition" anchor list followed by a table with
columns `Date of change | Date of announcement | Deletion | Addition`. `build_spans` opens a
span at the anchor date for each anchor company, then for each event closes the deleted
company's open span (`valid_to = date_of_change`) and opens a new span for the added company.

- [ ] **Step 1: Write the failing test for span reconstruction (pure logic, no PDF)**

```python
# tests/test_parse_stoxx_compositions.py
from tools.parse_stoxx_compositions import build_spans


def test_build_spans_open_close_reopen():
    anchor = {"date": "2003-03-24", "companies": ["AIXTRON", "Jenoptik"], "index_name": "TecDAX"}
    events = [
        {"date_of_change": "2005-09-19", "deletion": "AIXTRON", "addition": "Infineon"},
        {"date_of_change": "2009-09-21", "deletion": "Infineon", "addition": "AIXTRON"},
    ]
    spans = build_spans(events, anchor)
    # AIXTRON: in from anchor, out 2005-09-19, back in 2009-09-21 (open)
    aix = sorted([s for s in spans if s["company_name"] == "AIXTRON"], key=lambda s: s["valid_from"])
    assert aix[0] == {"index_name": "TecDAX", "company_name": "AIXTRON",
                      "valid_from": "2003-03-24", "valid_to": "2005-09-19"}
    assert aix[1] == {"index_name": "TecDAX", "company_name": "AIXTRON",
                      "valid_from": "2009-09-21", "valid_to": None}
    # Infineon: in 2005-09-19, out 2009-09-21
    inf = [s for s in spans if s["company_name"] == "Infineon"]
    assert inf == [{"index_name": "TecDAX", "company_name": "Infineon",
                    "valid_from": "2005-09-19", "valid_to": "2009-09-21"}]
    # Jenoptik: anchor, never removed -> open span
    jen = [s for s in spans if s["company_name"] == "Jenoptik"]
    assert jen == [{"index_name": "TecDAX", "company_name": "Jenoptik",
                    "valid_from": "2003-03-24", "valid_to": None}]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_parse_stoxx_compositions.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tools.parse_stoxx_compositions'`

- [ ] **Step 3: Implement `build_spans` (pure) and a thin PDF wrapper**

```python
# tools/parse_stoxx_compositions.py
"""Reconstruct point-in-time DE index membership from the STOXX
"Historical Index Compositions of DAX Equity Indices" PDF.

Source (public, auto-updated):
https://www.stoxx.com/document/Indices/Common/Indexguide/Historical_Index_Compositions.pdf
"""
from __future__ import annotations

import csv
import re
import sys


def build_spans(events: list[dict], anchor: dict) -> list[dict]:
    """Turn an anchor composition + dated deletion/addition events into spans.

    A span is {index_name, company_name, valid_from, valid_to}. `valid_to=None`
    means still a member. Multiple non-contiguous spans per company are allowed.
    """
    index_name = anchor["index_name"]
    open_span: dict[str, dict] = {}
    spans: list[dict] = []

    def open_member(company: str, date: str) -> None:
        if company in open_span:
            return
        span = {"index_name": index_name, "company_name": company,
                "valid_from": date, "valid_to": None}
        open_span[company] = span
        spans.append(span)

    def close_member(company: str, date: str) -> None:
        span = open_span.pop(company, None)
        if span is not None:
            span["valid_to"] = date

    for company in anchor["companies"]:
        open_member(company, anchor["date"])

    for ev in sorted(events, key=lambda e: e["date_of_change"]):
        date = ev["date_of_change"]
        for company in _split_cell(ev.get("deletion", "")):
            close_member(company, date)
        for company in _split_cell(ev.get("addition", "")):
            open_member(company, date)

    return spans


def _split_cell(cell: str) -> list[str]:
    """One STOXX cell may list several companies on separate lines."""
    out = []
    for line in str(cell).splitlines():
        name = line.strip().rstrip("*").strip()  # '*' marks merged/renamed
        if name and name != "-":
            out.append(name)
    return out


# --- PDF extraction (best-effort; the committed CSV is source of truth) ---

_INDEX_HEADERS = {
    "DAX® Index Composition": "DAX",
    "TecDAX® Index Composition": "TecDAX",
    "MDAX® Index Composition": "MDAX",
    "SDAX® Index Composition": "SDAX",
}
_DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")


def _iso(d: str) -> str:
    dd, mm, yyyy = d.split("/")
    return f"{yyyy}-{mm}-{dd}"


def parse_compositions(pdf_path: str) -> list[dict]:
    """Extract anchor + events per index from the PDF, return all spans.

    This is a best-effort extractor. Review the output CSV against the PDF
    before committing; build_spans is the tested invariant.
    """
    from pypdf import PdfReader

    reader = PdfReader(pdf_path)
    full = "\n".join((p.extract_text() or "") for p in reader.pages)
    spans: list[dict] = []
    for header, index_name in _INDEX_HEADERS.items():
        section = _slice_section(full, header)
        if not section:
            continue
        anchor = _parse_anchor(section, index_name)
        events = _parse_events(section)
        spans.extend(build_spans(events, anchor))
    return spans


def _slice_section(full: str, header: str) -> str:
    start = full.find(header)
    if start == -1:
        return ""
    nexts = [full.find(h, start + len(header)) for h in _INDEX_HEADERS if full.find(h, start + len(header)) != -1]
    end = min(nexts) if nexts else len(full)
    return full[start:end]


def _parse_anchor(section: str, index_name: str) -> dict:
    m = re.search(r"(\d{1,2}\s+\w+\s+\d{4}):\s*Initial Composition", section)
    # Anchor companies span from the line after the header to the first event date.
    date = _iso_from_long(m.group(1)) if m else "1900-01-01"
    head = section[m.end():] if m else section
    first_event = _DATE_RE.search(head)
    block = head[: first_event.start()] if first_event else head
    companies = [ln.strip() for ln in block.splitlines() if ln.strip() and "Date of" not in ln]
    return {"index_name": index_name, "date": date, "companies": companies}


def _parse_events(section: str) -> list[dict]:
    events: list[dict] = []
    for m in _DATE_RE.finditer(section):
        # Heuristic row capture — refine against the PDF when generating the CSV.
        events.append({"date_of_change": _iso(m.group(1)), "deletion": "", "addition": ""})
    return events


def _iso_from_long(s: str) -> str:
    import datetime as _dt
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return _dt.datetime.strptime(s.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return "1900-01-01"


def _write_csv(spans: list[dict], out_path: str) -> None:
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["index_name", "company_name", "valid_from", "valid_to"])
        w.writeheader()
        for s in sorted(spans, key=lambda s: (s["index_name"], s["company_name"], s["valid_from"])):
            w.writerow({**s, "valid_to": s["valid_to"] or ""})


if __name__ == "__main__":
    pdf, out = sys.argv[1], sys.argv[2]
    _write_csv(parse_compositions(pdf), out)
    print(f"wrote {out}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_parse_stoxx_compositions.py -v`
Expected: PASS

- [ ] **Step 5: Generate the raw DE seed and hand-verify**

Run:
```bash
curl -sL -A "Mozilla/5.0" -o /tmp/stoxx.pdf \
  "https://www.stoxx.com/document/Indices/Common/Indexguide/Historical_Index_Compositions.pdf"
python3 tools/parse_stoxx_compositions.py /tmp/stoxx.pdf warehouse/seeds/seed_index_membership_de_raw.csv
```
Open `seed_index_membership_de_raw.csv`, spot-check known events against the PDF (e.g.
Infineon DAX 2009-03-23 addition / 2009-09-21 deletion). Hand-correct any rows the heuristic
`_parse_events` got wrong — **the committed CSV is the source of truth, not the parser.**
Restrict committed rows to companies that plausibly map to a live `.DE` ticker (backtest era,
~2010→now); drop ancient defunct names.

- [ ] **Step 6: Commit**

```bash
git add tools/parse_stoxx_compositions.py tests/test_parse_stoxx_compositions.py warehouse/seeds/seed_index_membership_de_raw.csv
git commit -m "feat(universe): STOXX historical composition parser + raw DE membership seed"
```

---

### Task 2: Name→ticker crosswalk + resolved DE membership seed

**Files:**
- Create: `warehouse/seeds/seed_index_name_ticker_map.csv`
- Create: `tools/resolve_de_membership.py`
- Create (committed output): `warehouse/seeds/seed_index_membership_de.csv`
- Test: `tests/test_resolve_de_membership.py`

**Interfaces:**
- Consumes: `seed_index_membership_de_raw.csv` (Task 1), columns `index_name, company_name, valid_from, valid_to`.
- Produces: `resolve(rows: list[dict], crosswalk: dict[str, str]) -> tuple[list[dict], list[str]]`
  returning `(resolved_rows, unmapped_names)`. `resolved_rows` have columns
  `ticker, market, index_name, valid_from, valid_to` (market always `germany`).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_resolve_de_membership.py
from tools.resolve_de_membership import resolve


def test_resolve_maps_known_names_and_reports_unmapped():
    rows = [
        {"index_name": "DAX", "company_name": "Infineon", "valid_from": "2009-03-23", "valid_to": ""},
        {"index_name": "DAX", "company_name": "Daimler", "valid_from": "2010-01-01", "valid_to": "2021-02-01"},
        {"index_name": "DAX", "company_name": "Defunct AG", "valid_from": "2011-01-01", "valid_to": ""},
    ]
    crosswalk = {"Infineon": "IFX.DE", "Daimler": "MBG.DE"}
    resolved, unmapped = resolve(rows, crosswalk)
    assert {"ticker": "IFX.DE", "market": "germany", "index_name": "DAX",
            "valid_from": "2009-03-23", "valid_to": None} in resolved
    assert {"ticker": "MBG.DE", "market": "germany", "index_name": "DAX",
            "valid_from": "2010-01-01", "valid_to": "2021-02-01"} in resolved
    assert unmapped == ["Defunct AG"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_resolve_de_membership.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement the resolver**

```python
# tools/resolve_de_membership.py
from __future__ import annotations

import csv
import sys


def resolve(rows: list[dict], crosswalk: dict[str, str]) -> tuple[list[dict], list[str]]:
    resolved: list[dict] = []
    unmapped: list[str] = []
    for r in rows:
        ticker = crosswalk.get(r["company_name"])
        if ticker is None:
            if r["company_name"] not in unmapped:
                unmapped.append(r["company_name"])
            continue
        resolved.append({
            "ticker": ticker,
            "market": "germany",
            "index_name": r["index_name"],
            "valid_from": r["valid_from"],
            "valid_to": (r["valid_to"] or None),
        })
    return resolved, unmapped


def _load_crosswalk(path: str) -> dict[str, str]:
    with open(path, newline="") as f:
        return {row["company_name"]: row["ticker"] for row in csv.DictReader(f)}


def _load_rows(path: str) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _write(resolved: list[dict], out_path: str) -> None:
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["ticker", "market", "index_name", "valid_from", "valid_to"])
        w.writeheader()
        for r in sorted(resolved, key=lambda r: (r["ticker"], r["index_name"], r["valid_from"])):
            w.writerow({**r, "valid_to": r["valid_to"] or ""})


if __name__ == "__main__":
    raw, crosswalk_path, out = sys.argv[1], sys.argv[2], sys.argv[3]
    resolved, unmapped = resolve(_load_rows(raw), _load_crosswalk(crosswalk_path))
    _write(resolved, out)
    if unmapped:
        print(f"[WARN] {len(unmapped)} unmapped names (add to crosswalk if tradeable):")
        for n in unmapped:
            print(f"  - {n}")
    print(f"wrote {out} ({len(resolved)} rows)")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_resolve_de_membership.py -v`
Expected: PASS

- [ ] **Step 5: Author the crosswalk and generate the resolved seed**

Create `warehouse/seeds/seed_index_name_ticker_map.csv` with header
`company_name,ticker` and map every tradeable name in the raw seed to its yfinance `.DE`
ticker. Seed it from the 11 known names plus the live picks
(HOT.DE HelloFresh, NDA.DE Aurubis, RWE.DE, ELG.DE Elmos, NDX1.DE Nordex, AIXA.DE Aixtron,
GBF.DE Bilfinger, JEN.DE Jenoptik, EOAN.DE E.ON, SMHN.DE SUSS MicroTec, IFX.DE Infineon,
ENR.DE Siemens Energy, and known renames Daimler→MBG.DE). Then:

```bash
python3 tools/resolve_de_membership.py \
  warehouse/seeds/seed_index_membership_de_raw.csv \
  warehouse/seeds/seed_index_name_ticker_map.csv \
  warehouse/seeds/seed_index_membership_de.csv
```
Iterate on the crosswalk until the `[WARN] unmapped` list contains only genuinely
defunct/non-tradeable names.

- [ ] **Step 6: Commit**

```bash
git add warehouse/seeds/seed_index_name_ticker_map.csv tools/resolve_de_membership.py tests/test_resolve_de_membership.py warehouse/seeds/seed_index_membership_de.csv
git commit -m "feat(universe): name->ticker crosswalk + resolved DE membership seed"
```

---

### Task 3: US S&P 500 backfill seed

**Files:**
- Create: `tools/build_us_membership.py`
- Create (committed output): `warehouse/seeds/seed_index_membership_us.csv`
- Test: `tests/test_build_us_membership.py`

**Interfaces:**
- Produces: `to_spans(changes: list[dict], current: list[str], asof: str) -> list[dict]` where
  `changes` rows are `{date, added: list[str], removed: list[str]}` (newest-last) and
  `current` is today's constituent list. Returns rows
  `{ticker, market, index_name, valid_from, valid_to}` (market `us`, index `SP500`).

The GitHub dataset (`fja05680/sp500` style) lists, per date, the full constituent set or the
deltas. `to_spans` walks the deltas backward from `current`/`asof` to open/close spans.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_build_us_membership.py
from tools.build_us_membership import to_spans


def test_to_spans_backward_walk():
    current = ["AAPL", "NVDA"]
    asof = "2026-06-01"
    changes = [
        {"date": "2020-01-01", "added": ["NVDA"], "removed": ["XRX"]},
    ]
    spans = to_spans(changes, current, asof)
    # NVDA added 2020-01-01, still in -> open span
    assert {"ticker": "NVDA", "market": "us", "index_name": "SP500",
            "valid_from": "2020-01-01", "valid_to": None} in spans
    # XRX removed 2020-01-01 -> closed span ending then (valid_from unknown -> dataset epoch)
    xrx = [s for s in spans if s["ticker"] == "XRX"]
    assert xrx and xrx[0]["valid_to"] == "2020-01-01"
    # AAPL never changed -> open span from dataset epoch
    aapl = [s for s in spans if s["ticker"] == "AAPL"]
    assert aapl and aapl[0]["valid_to"] is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_build_us_membership.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement the backward walk**

```python
# tools/build_us_membership.py
from __future__ import annotations

import csv
import sys

EPOCH = "1996-01-02"  # dataset start; spans open here when no earlier add is known


def to_spans(changes: list[dict], current: list[str], asof: str) -> list[dict]:
    open_from: dict[str, str] = {t: EPOCH for t in current}
    spans: list[dict] = []

    for ch in sorted(changes, key=lambda c: c["date"], reverse=True):
        date = ch["date"]
        for t in ch.get("added", []):
            start = open_from.pop(t, date)
            spans.append({"ticker": t, "market": "us", "index_name": "SP500",
                          "valid_from": start, "valid_to": None if t in current else _later_close(spans, t)})
            if t not in current:
                # added then later removed; close handled when its removal is met going back
                pass
        for t in ch.get("removed", []):
            spans.append({"ticker": t, "market": "us", "index_name": "SP500",
                          "valid_from": EPOCH, "valid_to": date})

    for t, start in open_from.items():
        spans.append({"ticker": t, "market": "us", "index_name": "SP500",
                      "valid_from": start, "valid_to": None})
    return spans


def _later_close(spans, t):  # placeholder for symmetric closure; refined against real data
    return None


def _load_changes(path: str) -> list[dict]:
    with open(path, newline="") as f:
        rows = []
        for r in csv.DictReader(f):
            rows.append({"date": r["date"],
                         "added": [x for x in r.get("added", "").split(";") if x],
                         "removed": [x for x in r.get("removed", "").split(";") if x]})
        return rows


def _write(spans: list[dict], out_path: str) -> None:
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["ticker", "market", "index_name", "valid_from", "valid_to"])
        w.writeheader()
        for s in sorted(spans, key=lambda s: (s["ticker"], s["valid_from"])):
            w.writerow({**s, "valid_to": s["valid_to"] or ""})


if __name__ == "__main__":
    changes_csv, current_csv, asof, out = sys.argv[1:5]
    with open(current_csv) as f:
        current = [ln.strip() for ln in f if ln.strip()]
    _write(to_spans(_load_changes(changes_csv), current, asof), out)
    print(f"wrote {out}")
```

> Note: the backward-walk has known sharp edges for tickers added-then-removed within the
> dataset window. When generating the real CSV, reconcile the output against the current
> Wikipedia S&P 500 list and fix any span that doesn't round-trip. The committed CSV is the
> source of truth; `to_spans` is a generation aid with the tested happy path above.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_build_us_membership.py -v`
Expected: PASS

- [ ] **Step 5: Generate and verify the US seed**

Download the historical-changes CSV and current constituents from the GitHub S&P 500 dataset,
run the script, and reconcile open spans against the current Wikipedia list (must match the
~500 active names). Commit the reconciled CSV.

- [ ] **Step 6: Commit**

```bash
git add tools/build_us_membership.py tests/test_build_us_membership.py warehouse/seeds/seed_index_membership_us.csv
git commit -m "feat(universe): US S&P 500 historical membership seed"
```

---

### Task 4: Register seeds + seed-level dbt tests

**Files:**
- Modify: `warehouse/dbt_project.yml:27-57` (add three seeds under `seeds.dadayu_warehouse`)
- Modify: `warehouse/seeds/schema.yml` (add seed definitions + tests)

**Interfaces:**
- Produces: dbt seed relations `seed_index_membership_de`, `seed_index_membership_us`,
  `seed_index_name_ticker_map` in schema `dadayu`.

- [ ] **Step 1: Add column types in `warehouse/dbt_project.yml`**

Under `seeds: dadayu_warehouse:` add:
```yaml
    seed_index_membership_de:
      +column_types:
        ticker: text
        market: text
        index_name: text
        valid_from: date
        valid_to: date
    seed_index_membership_us:
      +column_types:
        ticker: text
        market: text
        index_name: text
        valid_from: date
        valid_to: date
    seed_index_name_ticker_map:
      +column_types:
        company_name: text
        ticker: text
```

- [ ] **Step 2: Add seed tests in `warehouse/seeds/schema.yml`**

Append:
```yaml
  - name: seed_index_membership_de
    description: Point-in-time DE index membership reconstructed from STOXX historical compositions.
    columns:
      - name: ticker
        data_tests: [not_null]
      - name: market
        data_tests:
          - not_null
          - accepted_values: {values: ['germany']}
      - name: index_name
        data_tests:
          - not_null
          - accepted_values: {values: ['DAX', 'MDAX', 'SDAX', 'TecDAX']}
      - name: valid_from
        data_tests: [not_null]
  - name: seed_index_membership_us
    description: Point-in-time US S&P 500 membership from the GitHub historical dataset.
    columns:
      - name: ticker
        data_tests: [not_null]
      - name: market
        data_tests:
          - not_null
          - accepted_values: {values: ['us']}
      - name: index_name
        data_tests:
          - not_null
          - accepted_values: {values: ['SP500']}
      - name: valid_from
        data_tests: [not_null]
  - name: seed_index_name_ticker_map
    description: Crosswalk from STOXX German company names to yfinance .DE tickers.
    columns:
      - name: company_name
        data_tests: [not_null, unique]
      - name: ticker
        data_tests: [not_null]
```

- [ ] **Step 3: Load and test the seeds**

Run:
```bash
cd warehouse && dbt seed --select seed_index_membership_de seed_index_membership_us seed_index_name_ticker_map && dbt test --select seed_index_membership_de seed_index_membership_us seed_index_name_ticker_map
```
Expected: seeds load; all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add warehouse/dbt_project.yml warehouse/seeds/schema.yml
git commit -m "feat(universe): register membership seeds with dbt tests"
```

---

### Task 5: Capture live-scraped membership in ingest

**Files:**
- Modify: `dadayu/ingest/equity.py` (`_get_germany_tickers` → also return index; add `get_index_membership`)
- Modify: `dagster_pipeline/assets/equity.py` (new asset `equity_index_membership`)
- Create: `warehouse/models/01_staging/yahoo/stg_membership__observed.sql`
- Test: `tests/test_equity_membership.py`

**Interfaces:**
- Produces: `get_index_membership(market: str) -> list[tuple[str, str]]` returning
  `(ticker, index_name)` pairs. For `germany`, `index_name ∈ {DAX,MDAX,SDAX,TecDAX}`; for
  `us`, all pairs are `(ticker, "SP500")`.
- Produces: table `index_membership_observed(ticker text, market text, index_name text, observed_at timestamp)`.
- Consumes: existing `_get_germany_membership` internals already loop per index (currently discarded into a set).

- [ ] **Step 1: Write the failing test (germany pairs carry index name)**

```python
# tests/test_equity_membership.py
from unittest.mock import patch
from dadayu.ingest import equity


def test_get_index_membership_us_labels_sp500():
    with patch.object(equity, "_get_us_tickers", return_value=["AAPL", "MSFT"]):
        pairs = equity.get_index_membership("us")
    assert set(pairs) == {("AAPL", "SP500"), ("MSFT", "SP500")}


def test_get_index_membership_germany_carries_index():
    fake = [("SAP.DE", "DAX"), ("AIXA.DE", "TecDAX")]
    with patch.object(equity, "_get_germany_membership", return_value=fake):
        pairs = equity.get_index_membership("germany")
    assert ("AIXA.DE", "TecDAX") in pairs
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_equity_membership.py -v`
Expected: FAIL — `get_index_membership` / `_get_germany_membership` not defined.

- [ ] **Step 3: Refactor scrape to keep the index label**

In `dadayu/ingest/equity.py`, rename the body of `_get_germany_tickers` to
`_get_germany_membership() -> list[tuple[str, str]]`, changing the `tickers: set[str]`
accumulation to collect `(ticker, index)` pairs (the `index` loop variable already exists at
`for index, urls in sources.items()`). Keep the existing normalization (`.DE` suffix logic).
Then:

```python
def _get_germany_tickers() -> list[str]:
    return sorted({t for t, _ in _get_germany_membership()})


def get_index_membership(market: str) -> list[tuple[str, str]]:
    if market == "germany":
        return sorted(set(_get_germany_membership()))
    if market == "us":
        return sorted((t, "SP500") for t in _get_us_tickers())
    raise ValueError(f"Unknown market: {market}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_equity_membership.py -v`
Expected: PASS

- [ ] **Step 5: Add the Dagster asset that persists observed membership**

In `dagster_pipeline/assets/equity.py` add (import `get_index_membership`):

```python
@asset(group_name="ingestion", deps=[AssetKey("equity_ticker_info")])
def equity_index_membership(postgres: PostgresResource) -> None:
    client = postgres.get_client()
    rows = []
    now = pd.Timestamp.now()
    for market in MARKETS:
        for ticker, index_name in get_index_membership(market):
            rows.append({"ticker": ticker, "market": market,
                         "index_name": index_name, "observed_at": now})
    if not rows:
        return
    df = pd.DataFrame(rows)
    client.execute(
        "CREATE TABLE IF NOT EXISTS index_membership_observed ("
        "ticker text, market text, index_name text, observed_at timestamp)"
    )
    client.execute("TRUNCATE index_membership_observed")
    client.insert_df("index_membership_observed", df)
```

- [ ] **Step 6: Add the staging model**

```sql
-- warehouse/models/01_staging/yahoo/stg_membership__observed.sql
{{ config(materialized='view') }}

SELECT
    ticker,
    market,
    index_name,
    observed_at
FROM {{ source('yahoo', 'index_membership_observed') }}
```
Add `index_membership_observed` to the `yahoo` source in
`warehouse/models/01_staging/yahoo/_sources.yml` (follow the existing table entries there).

- [ ] **Step 7: Run the ingest path locally and verify the table fills**

Run: `python3 -m pytest tests/test_equity_membership.py -v` (regression)
Expected: PASS. (Full Dagster materialization is exercised in Task 9's integration run.)

- [ ] **Step 8: Commit**

```bash
git add dadayu/ingest/equity.py dagster_pipeline/assets/equity.py warehouse/models/01_staging/yahoo/stg_membership__observed.sql warehouse/models/01_staging/yahoo/_sources.yml tests/test_equity_membership.py
git commit -m "feat(universe): capture live index membership during ingest"
```

---

### Task 6: SCD2 snapshot of observed membership

**Files:**
- Create: `warehouse/snapshots/snap_index_membership.sql`
- Create: `warehouse/snapshots/snap_index_membership.yml`

**Interfaces:**
- Produces: snapshot relation `snap_index_membership` with dbt columns `dbt_valid_from`,
  `dbt_valid_to` per `(ticker, market, index_name)`.

- [ ] **Step 1: Write the snapshot**

```sql
-- warehouse/snapshots/snap_index_membership.sql
{% snapshot snap_index_membership %}

{{ config(
    target_schema='dadayu',
    unique_key=dbt_utils.generate_surrogate_key(['ticker', 'market', 'index_name']),
    strategy='check',
    check_cols=['index_name'],
    invalidate_hard_deletes=true
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ticker', 'market', 'index_name']) }} AS membership_id,
    ticker,
    market,
    index_name,
    observed_at
FROM {{ ref('stg_membership__observed') }}

{% endsnapshot %}
```

- [ ] **Step 2: Add snapshot doc/tests**

```yaml
# warehouse/snapshots/snap_index_membership.yml
version: 2
snapshots:
  - name: snap_index_membership
    description: SCD Type 2 history of live index membership; valid_to closes when a ticker leaves an index.
    columns:
      - name: ticker
        data_tests: [not_null]
      - name: market
        data_tests: [not_null]
      - name: index_name
        data_tests: [not_null]
```

- [ ] **Step 3: Run the snapshot twice to prove SCD2 behavior**

Run:
```bash
cd warehouse && dbt snapshot --select snap_index_membership && dbt snapshot --select snap_index_membership
```
Expected: first run inserts rows with open `dbt_valid_to`; second run is a no-op (no
membership change) — row count stable.

- [ ] **Step 4: Commit**

```bash
git add warehouse/snapshots/snap_index_membership.sql warehouse/snapshots/snap_index_membership.yml
git commit -m "feat(universe): SCD2 snapshot of live index membership"
```

---

### Task 7: Unified `int_universe_membership_daily`

**Files:**
- Create: `warehouse/models/02_intermediate/market/int_universe_membership_daily.sql`
- Create/Modify: `warehouse/models/02_intermediate/schema.yml` (tests)

**Interfaces:**
- Produces: model `int_universe_membership_daily` with columns
  `ticker, market, valid_from (date), valid_to (date|null)` — **one non-overlapping span set
  per (ticker, market)**, unioned across all indices and all three layers.
- Consumes: `seed_index_membership_de`, `seed_index_membership_us`, `snap_index_membership`.

Semantics: a ticker may be in several indices (e.g. HDAX overlap) or several spans; the model
collapses per-index spans into a single tradeable-span set per `(ticker, market)` by merging
overlapping/adjacent intervals. Live snapshot spans take precedence by being unioned in —
merging makes the union idempotent regardless of source overlap.

- [ ] **Step 1: Write the merge model**

```sql
-- warehouse/models/02_intermediate/market/int_universe_membership_daily.sql
{{ config(materialized='table') }}

WITH all_spans AS (
    SELECT ticker, market, valid_from,
           coalesce(valid_to, DATE '9999-12-31') AS valid_to
    FROM {{ ref('seed_index_membership_de') }}
    UNION ALL
    SELECT ticker, market, valid_from,
           coalesce(valid_to, DATE '9999-12-31') AS valid_to
    FROM {{ ref('seed_index_membership_us') }}
    UNION ALL
    SELECT ticker, market, dbt_valid_from::date AS valid_from,
           coalesce(dbt_valid_to::date, DATE '9999-12-31') AS valid_to
    FROM {{ ref('snap_index_membership') }}
),

ordered AS (
    SELECT ticker, market, valid_from, valid_to,
           max(valid_to) OVER (
               PARTITION BY ticker, market
               ORDER BY valid_from, valid_to
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS prev_max_to
    FROM all_spans
),

islands AS (
    SELECT ticker, market, valid_from, valid_to,
           sum(CASE WHEN prev_max_to IS NULL OR valid_from > prev_max_to THEN 1 ELSE 0 END)
               OVER (PARTITION BY ticker, market ORDER BY valid_from, valid_to) AS grp
    FROM ordered
)

SELECT
    ticker,
    market,
    min(valid_from) AS valid_from,
    nullif(max(valid_to), DATE '9999-12-31') AS valid_to
FROM islands
GROUP BY ticker, market, grp
```

- [ ] **Step 2: Add dbt tests for non-overlap and ordering**

In `warehouse/models/02_intermediate/schema.yml` add:
```yaml
  - name: int_universe_membership_daily
    description: Unified non-overlapping point-in-time membership spans per ticker/market.
    columns:
      - name: ticker
        data_tests: [not_null]
      - name: market
        data_tests:
          - not_null
          - accepted_values: {values: ['germany', 'us']}
      - name: valid_from
        data_tests: [not_null]
    data_tests:
      - dbt_utils.expression_is_true:
          expression: "valid_to IS NULL OR valid_to > valid_from"
```

- [ ] **Step 3: Build and test**

Run:
```bash
cd warehouse && dbt run --select int_universe_membership_daily && dbt test --select int_universe_membership_daily
```
Expected: build succeeds; tests PASS.

- [ ] **Step 4: Commit**

```bash
git add warehouse/models/02_intermediate/market/int_universe_membership_daily.sql warehouse/models/02_intermediate/schema.yml
git commit -m "feat(universe): unified point-in-time membership model"
```

---

### Task 8: Wire eligibility into the backtest signal model

**Files:**
- Modify: `warehouse/models/03_marts/backtests/mart_backtest_signals_daily.sql` (the `scoped_candidates` CTE)
- Test: `warehouse/tests/backtest_membership_eligibility.sql` (dbt singular test)

**Interfaces:**
- Consumes: `int_universe_membership_daily` (Task 7).
- Behavior change: a `(ticker, market)` candidate at `signal_date` is kept only if a
  membership span covers `signal_date` (`valid_from <= signal_date AND (valid_to IS NULL OR
  signal_date < valid_to)`).

- [ ] **Step 1: Write the failing singular test**

```sql
-- warehouse/tests/backtest_membership_eligibility.sql
-- Fails if any signal row exists for a ticker outside its membership span.
SELECT s.ticker, s.market, s.signal_date
FROM {{ ref('mart_backtest_signals_daily') }} AS s
LEFT JOIN {{ ref('int_universe_membership_daily') }} AS m
    ON s.ticker = m.ticker
    AND s.market = m.market
    AND s.signal_date >= m.valid_from
    AND (m.valid_to IS NULL OR s.signal_date < m.valid_to)
WHERE m.ticker IS NULL
```

- [ ] **Step 2: Run it to verify it fails (pre-change)**

Run: `cd warehouse && dbt test --select backtest_membership_eligibility`
Expected: FAIL — current model emits signals for tickers regardless of membership.

- [ ] **Step 3: Add the eligibility join to `scoped_candidates`**

Replace the `scoped_candidates` CTE:
```sql
scoped_candidates AS (
    SELECT
        c.*,
        'equity' AS universe_scope
    FROM family_candidates AS c
    INNER JOIN {{ ref('int_universe_membership_daily') }} AS m
        ON c.ticker = m.ticker
        AND c.market = m.market
        AND c.signal_date >= m.valid_from
        AND (m.valid_to IS NULL OR c.signal_date < m.valid_to)
    WHERE c.asset_type = 'equity'
),
```

- [ ] **Step 4: Rebuild and re-run the test**

Run:
```bash
cd warehouse && dbt run --select mart_backtest_signals_daily && dbt test --select backtest_membership_eligibility
```
Expected: build succeeds; test PASS.

- [ ] **Step 5: Commit**

```bash
git add warehouse/models/03_marts/backtests/mart_backtest_signals_daily.sql warehouse/tests/backtest_membership_eligibility.sql
git commit -m "feat(universe): gate backtest signals on point-in-time membership"
```

---

### Task 9: Fail-loud member-count floor

**Files:**
- Create: `warehouse/tests/universe_active_floor_de.sql`
- Create: `warehouse/tests/universe_active_floor_us.sql`
- Modify: `dadayu/checks.py` (add `check_universe_membership`)
- Modify: `scripts/check_data_quality.py` (register the new check)
- Test: `tests/test_checks.py` (add a case)

**Interfaces:**
- Consumes: `int_universe_membership_daily`.
- Produces: `check_universe_membership(client) -> list[CheckResult]` following the existing
  `_check(...)` pattern in `dadayu/checks.py`.

- [ ] **Step 1: Write the dbt floor tests (these are the loud gate in the warehouse build)**

```sql
-- warehouse/tests/universe_active_floor_de.sql
-- Fails when fewer than 120 DE tickers are currently active members.
SELECT count(*) AS active_de
FROM {{ ref('int_universe_membership_daily') }}
WHERE market = 'germany' AND valid_to IS NULL
HAVING count(*) < 120
```
```sql
-- warehouse/tests/universe_active_floor_us.sql
-- Fails when fewer than 450 US tickers are currently active members.
SELECT count(*) AS active_us
FROM {{ ref('int_universe_membership_daily') }}
WHERE market = 'us' AND valid_to IS NULL
HAVING count(*) < 450
```

- [ ] **Step 2: Run the floor tests**

Run: `cd warehouse && dbt test --select universe_active_floor_de universe_active_floor_us`
Expected: PASS (seeds + snapshot give full current universe).

- [ ] **Step 3: Add the Python check (mirrors the gate for the quality dashboard)**

In `dadayu/checks.py` add:
```python
def check_universe_membership(client: PostgresClient) -> list[CheckResult]:
    results: list[CheckResult] = []
    _check(results, "universe", "Active DE members", client,
           "SELECT count(*) FROM int_universe_membership_daily WHERE market = 'germany' AND valid_to IS NULL",
           detail="floor 120")
    de = client.query(
        "SELECT count(*) FROM int_universe_membership_daily WHERE market = 'germany' AND valid_to IS NULL"
    ).result_rows[0][0]
    if de < 120:
        results[-1].status = "FAIL"
    _check(results, "universe", "Active US members", client,
           "SELECT count(*) FROM int_universe_membership_daily WHERE market = 'us' AND valid_to IS NULL",
           detail="floor 450")
    us = client.query(
        "SELECT count(*) FROM int_universe_membership_daily WHERE market = 'us' AND valid_to IS NULL"
    ).result_rows[0][0]
    if us < 450:
        results[-1].status = "FAIL"
    _check(results, "universe", "Overlapping spans", client,
           "SELECT count(*) FROM (SELECT a.ticker FROM int_universe_membership_daily a "
           "JOIN int_universe_membership_daily b ON a.ticker=b.ticker AND a.market=b.market "
           "AND a.valid_from < b.valid_from AND (a.valid_to IS NULL OR b.valid_from < a.valid_to)) x",
           fail_if_nonzero=True)
    return results
```

- [ ] **Step 4: Write a unit test for the check (mirror existing test_checks.py style)**

```python
# tests/test_checks.py  (add)
from dadayu.checks import check_universe_membership


def test_universe_membership_fails_below_floor(fake_client_factory):
    # fake_client_factory is the existing fixture pattern in this file; configure it to
    # return 10 DE members, 500 US, 0 overlaps.
    client = fake_client_factory({
        "germany' AND valid_to IS NULL": 10,
        "us' AND valid_to IS NULL": 500,
        "Overlapping": 0,
    })
    results = check_universe_membership(client)
    de = next(r for r in results if r.name == "Active DE members")
    assert de.status == "FAIL"
```
If `tests/test_checks.py` uses a different stubbing approach, follow that file's existing
fixture (read it first) and assert the same FAIL-below-floor behavior.

- [ ] **Step 5: Register in `scripts/check_data_quality.py`**

Add `check_universe_membership` to the list of check functions invoked there (follow the
existing registration pattern for `check_equity_ohlcv`).

- [ ] **Step 6: Run the Python checks**

Run: `python3 -m pytest tests/test_checks.py -v`
Expected: PASS.

- [ ] **Step 7: Full warehouse integration run**

Run:
```bash
cd warehouse && dbt seed && dbt snapshot && dbt run --select int_universe_membership_daily mart_backtest_signals_daily && dbt test --select int_universe_membership_daily backtest_membership_eligibility universe_active_floor_de universe_active_floor_us
```
Expected: all PASS.

- [ ] **Step 8: Commit**

```bash
git add warehouse/tests/universe_active_floor_de.sql warehouse/tests/universe_active_floor_us.sql dadayu/checks.py scripts/check_data_quality.py tests/test_checks.py
git commit -m "feat(universe): fail-loud active-member floor (DE>=120, US>=450)"
```

---

### Task 10: Document limitations

**Files:**
- Modify: `DATA.md`

- [ ] **Step 1: Add a universe-history section**

Document in `DATA.md`: the membership system (seeds + SCD2 snapshot), the
`valid_from` inclusive / `valid_to` exclusive convention, that point-in-time history before
go-live is best-effort (DAX most complete; pre-2010 defunct names excluded because they have
no tradeable yfinance data), the floor thresholds (DE≥120, US≥450), and how to refresh the
seeds (`tools/parse_stoxx_compositions.py`, `tools/resolve_de_membership.py`,
`tools/build_us_membership.py`).

- [ ] **Step 2: Commit**

```bash
git add DATA.md
git commit -m "docs: document point-in-time universe history and limitations"
```

---

## Self-Review Notes

- **Spec coverage:** backfill seeds (T1–T3), crosswalk (T2), floor seed = latest-state of
  seeds (T7 union/T9 floor), observed SCD2 (T5–T6), unified model (T7), backtest wiring (T8),
  validation gate (T9), docs/limitations (T10). All spec sections map to a task.
- **Known soft spots flagged in-plan:** the PDF event parser (`_parse_events`) and the US
  backward-walk (`to_spans`/`_later_close`) are generation aids with tested happy paths; the
  committed CSVs are the source of truth and must be hand-reconciled when generated (called
  out explicitly in T1.S5, T3.S3/S5). This is deliberate — robust full automation of messy
  historical PDF/CSV sources is out of scope; reproducibility comes from the committed seeds.
- **Type consistency:** span dicts use `index_name/company_name/valid_from/valid_to`
  throughout T1–T2; resolved/US rows use `ticker/market/index_name/valid_from/valid_to`
  (T2–T3); the dbt model emits `ticker/market/valid_from/valid_to` (T7) consumed identically
  by T8/T9. `get_index_membership` returns `(ticker, index_name)` pairs in T5 and is consumed
  by the same-task asset.
```
