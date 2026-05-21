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
