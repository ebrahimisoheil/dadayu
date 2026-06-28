from __future__ import annotations

import csv
import sys

EPOCH = "1996-01-02"  # dataset start; spans open here when no earlier add is known


def to_spans(changes: list[dict], current: list[str], asof: str) -> list[dict]:
    open_from: dict[str, str] = {t: EPOCH for t in current}
    spans: list[dict] = []
    # Tickers we have already seen a removal for (going backward means we see removes before adds)
    removed_set: set[str] = set()

    for ch in sorted(changes, key=lambda c: c["date"], reverse=True):
        date = ch["date"]
        for t in ch.get("added", []):
            if t in open_from:
                # Currently active: found its add date — open span from here
                open_from.pop(t)
                spans.append({"ticker": t, "market": "us", "index_name": "SP500",
                              "valid_from": date, "valid_to": None})
            elif t in removed_set:
                # Was removed later (saw it going backward); update its span's valid_from to add date
                for s in spans:
                    if s["ticker"] == t and s["valid_to"] is not None and s["valid_from"] == EPOCH:
                        s["valid_from"] = date
                        break
                removed_set.discard(t)
            # else: no record of this ticker; skip (best-effort)
        for t in ch.get("removed", []):
            removed_set.add(t)
            spans.append({"ticker": t, "market": "us", "index_name": "SP500",
                          "valid_from": EPOCH, "valid_to": date})

    for t, start in open_from.items():
        spans.append({"ticker": t, "market": "us", "index_name": "SP500",
                      "valid_from": start, "valid_to": None})
    return spans


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
