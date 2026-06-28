from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import pandas as pd
import psycopg
from psycopg import sql
from dotenv import load_dotenv

load_dotenv()


@dataclass
class QueryResult:
    result_rows: list[tuple[Any, ...]]
    column_names: list[str]


class PostgresClient:
    def __init__(self) -> None:
        self.connection = psycopg.connect(
            host=os.environ.get("DADAYU_PG_HOST", "localhost"),
            port=int(os.environ.get("DADAYU_PG_PORT", 5432)),
            dbname=os.environ.get("DADAYU_PG_DB", "dadayu"),
            user=os.environ.get("DADAYU_PG_USER", "dadayu"),
            password=os.environ.get("DADAYU_PG_PASSWORD", "dadayu"),
        )
        self.connection.autocommit = False

    def close(self) -> None:
        self.connection.close()

    def query(self, query: str, parameters: dict[str, Any] | None = None) -> QueryResult:
        with self.connection.cursor() as cur:
            cur.execute(query, parameters or {})
            rows = cur.fetchall()
            column_names = [desc.name for desc in cur.description or []]
        return QueryResult(result_rows=rows, column_names=column_names)

    def execute(self, query: str, parameters: dict[str, Any] | None = None) -> None:
        with self.connection.cursor() as cur:
            cur.execute(query, parameters or {})
        self.connection.commit()

    def insert_df(self, table: str, df: pd.DataFrame) -> None:
        self.upsert_df(table, df, conflict_cols=[])

    def upsert_df(
        self,
        table: str,
        df: pd.DataFrame,
        *,
        conflict_cols: list[str],
        update_cols: list[str] | None = None,
    ) -> None:
        if df.empty:
            return

        data = df.where(pd.notnull(df), None)
        columns = list(data.columns)
        rows = [tuple(row) for row in data.itertuples(index=False, name=None)]
        update_cols = update_cols or [col for col in columns if col not in conflict_cols]

        base = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({placeholders})").format(
            table=sql.Identifier(table),
            fields=sql.SQL(", ").join(sql.Identifier(col) for col in columns),
            placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )

        if conflict_cols:
            if update_cols:
                assignments = sql.SQL(", ").join(
                    sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
                    for col in update_cols
                )
                statement = base + sql.SQL(" ON CONFLICT ({conflict}) DO UPDATE SET {assignments}").format(
                    conflict=sql.SQL(", ").join(sql.Identifier(col) for col in conflict_cols),
                    assignments=assignments,
                )
            else:
                statement = base + sql.SQL(" ON CONFLICT ({conflict}) DO NOTHING").format(
                    conflict=sql.SQL(", ").join(sql.Identifier(col) for col in conflict_cols)
                )
        else:
            statement = base

        with self.connection.cursor() as cur:
            cur.executemany(statement, rows)
        self.connection.commit()


def get_pg_client() -> PostgresClient:
    return PostgresClient()
