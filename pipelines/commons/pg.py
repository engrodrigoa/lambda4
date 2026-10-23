"""Postgres for data (not audit): connections and a fast bulk load of Polars frames with COPY.

Audit has its own best-effort sink (pipelines/commons/audit.py); this module is for data writes, which must fail
loudly. Connections use the local socket with peer auth (LAMBDA_PG_DSN), so there is no password anywhere.
"""
from __future__ import annotations

import io
from typing import TYPE_CHECKING

from pipelines.commons.settings import get_settings

if TYPE_CHECKING:
    import polars as pl
    import psycopg

COPY_BATCH_ROWS = 200_000          # frame slice serialised per COPY write (bounded memory for big files)


def connect(dsn: str | None = None, **kw) -> psycopg.Connection:
    """Autocommit by default: every `with conn.transaction():` is then a real transaction (committed on exit), not a
    savepoint inside an implicit transaction that would only commit when the connection closes."""
    import psycopg

    kw.setdefault("autocommit", True)
    return psycopg.connect(dsn or get_settings().pg_dsn, **kw)


def _ident(name: str) -> str:
    """schema.table / column names come from code (contracts), never from data; still, refuse anything odd."""
    if not name.replace("_", "").replace(".", "").isalnum():
        raise ValueError(f"unexpected identifier: {name!r}")
    return ".".join(f'"{part}"' for part in name.split("."))


def copy_frame(cur: psycopg.Cursor, table: str, df: pl.DataFrame, batch_rows: int = COPY_BATCH_ROWS) -> int:
    """COPY `df` into `table` (columns by name, same order as the frame) inside the caller's transaction.
    Nulls travel as unquoted empty fields; empty strings stay empty strings (CSV quoting keeps them apart)."""
    if df.is_empty():
        return 0
    columns = ", ".join(_ident(c) for c in df.columns)
    sql = f"COPY {_ident(table)} ({columns}) FROM STDIN WITH (FORMAT csv, NULL '')"
    written = 0
    with cur.copy(sql) as copy:
        for offset in range(0, df.height, batch_rows):
            part = df.slice(offset, batch_rows)
            buf = io.BytesIO()
            # quote_style="non_numeric" quotes every string (even ""), so only real nulls are unquoted empties
            part.write_csv(buf, include_header=False, null_value="", quote_style="non_numeric")
            copy.write(buf.getvalue())
            written += part.height
    return written
