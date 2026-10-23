"""Control state of file-based sources in Postgres (schema ctrl, infra/postgres/anp_schema.sql). ONLY control.

* ctrl.source_file        - current state per (pipeline, file_key): discovered -> downloaded -> loaded.
* ctrl.source_file_event  - append-only history of every state change (when a file was seen, downloaded, loaded).

Generic across scraped sources (column `pipeline`). The stores only persist rows: deciding what changed is the
caller's job (pure functions, unit tested). Writes accept the caller's cursor, so a bronze load can update its
control row in the SAME transaction as the data (ADR 0006). `InMemoryFileCtrl` has the same interface for tests.
"""
from __future__ import annotations

import copy
from datetime import UTC, datetime
from typing import Any, Protocol

from psycopg.types.json import Jsonb

SOURCE_FILE_COLUMNS = (
    "pipeline", "file_key", "source_kind", "product_group", "period_start", "period_end", "url", "link_text",
    "listed", "first_seen_at", "last_seen_at", "remote_bytes", "remote_checked_at",
    "download_status", "download_attempts", "version", "local_path", "local_bytes", "sha256", "file_format",
    "downloaded_at", "duplicate_of",
    "bronze_status", "bronze_sha256", "bronze_rows", "bronze_quarantined", "loaded_at",
    "last_run_id", "last_error", "updated_at",
)
EVENT_COLUMNS = ("pipeline", "file_key", "event", "run_id", "task_id", "created_at", "version", "bytes", "sha256",
                 "rows", "detail")
DEFAULTS: dict[str, Any] = {"listed": True, "download_status": "PENDING", "download_attempts": 0, "version": 0,
                            "bronze_status": "PENDING"}


def now_utc() -> datetime:
    return datetime.now(UTC)


def new_row(**fields: Any) -> dict[str, Any]:
    row = dict.fromkeys(SOURCE_FILE_COLUMNS)
    row.update(DEFAULTS)
    row.update(fields)
    # the upsert sends every column, and an explicit NULL skips the column DEFAULT: stamp them here
    now = now_utc()
    row["first_seen_at"] = row["first_seen_at"] or now
    row["last_seen_at"] = row["last_seen_at"] or now
    unknown = set(row) - set(SOURCE_FILE_COLUMNS)
    if unknown:
        raise KeyError(f"unknown ctrl.source_file columns: {sorted(unknown)}")
    return row


def event(pipeline: str, file_key: str, name: str, *, run_id: str | None, task_id: str | None,
          **fields: Any) -> dict[str, Any]:
    row = dict.fromkeys(EVENT_COLUMNS)
    row.update(pipeline=pipeline, file_key=file_key, event=name, run_id=run_id, task_id=task_id,
               created_at=now_utc(), **fields)
    unknown = set(row) - set(EVENT_COLUMNS)
    if unknown:
        raise KeyError(f"unknown ctrl.source_file_event columns: {sorted(unknown)}")
    return row


class FileCtrl(Protocol):
    def load(self, pipeline: str) -> dict[str, dict[str, Any]]: ...
    def save(self, rows: list[dict[str, Any]], cur=None) -> None: ...
    def add_events(self, rows: list[dict[str, Any]], cur=None) -> None: ...


_UPSERT = (
    f"INSERT INTO ctrl.source_file ({', '.join(SOURCE_FILE_COLUMNS)}) "
    f"VALUES ({', '.join('%(' + c + ')s' for c in SOURCE_FILE_COLUMNS)}) "
    f"ON CONFLICT (pipeline, file_key) DO UPDATE SET "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in SOURCE_FILE_COLUMNS if c not in ("pipeline", "file_key"))
)
_EVENT = (f"INSERT INTO ctrl.source_file_event ({', '.join(EVENT_COLUMNS)}) "
          f"VALUES ({', '.join('%(' + c + ')s' for c in EVENT_COLUMNS)})")


class PgFileCtrl:
    def __init__(self, conn):
        # autocommit connection (pipelines.commons.pg.connect): no read transaction stays open during downloads
        self.conn = conn

    def load(self, pipeline: str) -> dict[str, dict[str, Any]]:
        from psycopg.rows import dict_row

        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"SELECT {', '.join(SOURCE_FILE_COLUMNS)} FROM ctrl.source_file WHERE pipeline = %s "
                        "ORDER BY file_key", (pipeline,))
            return {r["file_key"]: dict(r) for r in cur.fetchall()}

    def _run(self, sql: str, rows: list[dict[str, Any]], cur) -> None:
        if not rows:
            return
        if cur is not None:
            cur.executemany(sql, rows)
            return
        with self.conn.transaction(), self.conn.cursor() as own:
            own.executemany(sql, rows)

    def save(self, rows: list[dict[str, Any]], cur=None) -> None:
        self._run(_UPSERT, [{**r, "updated_at": now_utc()} for r in rows], cur)

    def add_events(self, rows: list[dict[str, Any]], cur=None) -> None:
        self._run(_EVENT, [{**r, "detail": Jsonb(r["detail"]) if r.get("detail") is not None else None}
                           for r in rows], cur)


class InMemoryFileCtrl:
    """Same interface, no database: unit tests and `--ctrl memory` dry runs."""

    def __init__(self, rows: list[dict[str, Any]] | None = None):
        self.rows: dict[tuple[str, str], dict[str, Any]] = {(r["pipeline"], r["file_key"]): r for r in rows or []}
        self.events: list[dict[str, Any]] = []

    def load(self, pipeline: str) -> dict[str, dict[str, Any]]:
        return {k: copy.deepcopy(r) for (p, k), r in sorted(self.rows.items()) if p == pipeline}

    def save(self, rows: list[dict[str, Any]], cur=None) -> None:
        for r in rows:
            self.rows[(r["pipeline"], r["file_key"])] = {**copy.deepcopy(r), "updated_at": now_utc()}

    def add_events(self, rows: list[dict[str, Any]], cur=None) -> None:
        self.events.extend(copy.deepcopy(rows))
