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
