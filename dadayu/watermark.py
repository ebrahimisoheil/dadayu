from __future__ import annotations

import pandas as pd

from dadayu.db import PostgresClient


def get_watermark(
    client: PostgresClient,
    table: str,
    date_col: str,
    market: str | None = None,
) -> str | None:
    try:
        if market is not None:
            result = client.query(
                f"SELECT max({date_col}) FROM {table} WHERE market = %(market)s",
                parameters={"market": market},
            )
        else:
            result = client.query(f"SELECT max({date_col}) FROM {table}")
        val = result.result_rows[0][0]
        if val is None:
            return None
        ts = pd.Timestamp(val)
        return (ts + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception as exc:
        print(f"  [WARN] Watermark query failed: {exc}")
        return None
