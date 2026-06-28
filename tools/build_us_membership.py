from __future__ import annotations

import csv
import sys

EPOCH = "1996-01-02"  # dataset start; spans open here when no earlier add is known


def to_spans(changes: list[dict], current: list[str], asof: str) -> list[dict]:
    # Issue 1: cap the backward walk at asof so future changes are excluded.
    filtered = [ch for ch in changes if ch["date"] <= asof]

    open_from: dict[str, str] = {t: EPOCH for t in current}
    # Issue 2: O(1) per-ticker span tracking — replaces the O(n²) removed_set linear scan.
    # open_spans holds the currently-open (not-yet-closed) span for each ticker that has been
    # seen in a "removed" event but whose matching "added" event has not been found yet.
    open_spans: dict[str, dict] = {}
    closed: list[dict] = []

    for ch in sorted(filtered, key=lambda c: c["date"], reverse=True):
        date = ch["date"]
        for t in ch.get("added", []):
            if t in open_from:
                # Currently active: found its add date — emit open span from here.
                open_from.pop(t)
                closed.append({"ticker": t, "market": "us", "index_name": "SP500",
                               "valid_from": date, "valid_to": None})
            elif t in open_spans:
                # Was removed later (saw removal going backward); close the span now.
                span = open_spans.pop(t)
                span["valid_from"] = date
                closed.append(span)
            # else: no record of this ticker; skip (best-effort).
        for t in ch.get("removed", []):
            # Open a span with placeholder valid_from=EPOCH; it will be updated when we
            # find the matching "added" event further back.  Using a dict gives O(1) lookup
            # and correctly handles tickers with 3+ episodes (each add closes the current
            # open_spans entry, freeing the key for the next removal going further back).
            open_spans[t] = {"ticker": t, "market": "us", "index_name": "SP500",
                             "valid_from": EPOCH, "valid_to": date}

    # Current tickers whose add event was not found — span from dataset epoch.
    for t, start in open_from.items():
        closed.append({"ticker": t, "market": "us", "index_name": "SP500",
                       "valid_from": start, "valid_to": None})

    # Removed tickers whose add event was not found — span from dataset epoch.
    closed.extend(open_spans.values())

    return closed


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
