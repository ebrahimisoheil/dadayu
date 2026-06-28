#!/usr/bin/env python3
"""
Ad-hoc data quality checks for DADAYU pipeline.
Usage: python scripts/check_data_quality.py
"""
from __future__ import annotations

import sys

from dadayu.db import PostgresClient, get_pg_client
from dadayu.checks import CheckResult, run_all_checks


def get_client() -> PostgresClient:
    return get_pg_client()


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
    print("Connecting to PostgreSQL...")
    client = get_client()
    print("Connected to DADAYU PostgreSQL warehouse")

    results = run_all_checks(client)
    exit_code = print_report(results)
    sys.exit(exit_code)
