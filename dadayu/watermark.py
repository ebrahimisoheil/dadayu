from __future__ import annotations

import clickhouse_connect
import pandas as pd


def get_watermark(
    client: clickhouse_connect.driver.Client,
    table: str,
    date_col: str,
    market: str | None = None,
    condition_id: str | None = None,
) -> str | None:
    try:
        if market is not None:
            result = client.query(
                f"SELECT max({date_col}) FROM {table} WHERE market = {{market:String}}",
                parameters={"market": market},
            )
        elif condition_id is not None:
            result = client.query(
                f"SELECT max({date_col}) FROM {table} WHERE condition_id = {{condition_id:String}}",
                parameters={"condition_id": condition_id},
            )
        else:
            result = client.query(f"SELECT max({date_col}) FROM {table}")
        val = result.result_rows[0][0]
        if val is None:
            return None
        return (pd.Timestamp(val) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception as exc:
        print(f"  [WARN] Watermark query failed: {exc}")
        return None
