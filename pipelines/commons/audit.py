"""Best-effort writer of the Postgres `audit` schema (infra/postgres/audit_schema.sql).

Rules:
* It never raises into the pipeline. A failed write is spooled to LAMBDA_AUDIT_SPOOL_DIR as JSONL and replayed, in
  order, the next time a sink connects. Timestamps are captured when the event happens (not at replay time).
* Once the database is found down, the sink stops trying for the rest of the process (no timeout per event).
* Only operations named in `SQL` exist, so a spooled line can always be replayed by a newer version of this module.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipelines.commons.settings import get_settings

# internal logger: never mirrored back into audit (see logger.AuditHandler) to avoid loops
_log = logging.getLogger("lambda.audit")

JSON_PARAMS = {"params", "metrics", "context", "fields", "detail", "op_metrics"}

SQL: dict[str, str] = {
    "pipeline_run_start": """
        INSERT INTO audit.pipeline_run (run_id, pipeline, dag_id, ref_month, trigger, status, started_at, host,
                                        git_sha, params)
        VALUES (%(run_id)s, %(pipeline)s, %(dag_id)s, %(ref_month)s, %(trigger)s, 'RUNNING', %(started_at)s,
                %(host)s, %(git_sha)s, %(params)s)
        ON CONFLICT (run_id) DO NOTHING""",
    "pipeline_run_finish": """
        UPDATE audit.pipeline_run p SET
            finished_at  = %(finished_at)s,
            ref_month    = COALESCE(p.ref_month, %(ref_month)s),
            tasks_total  = s.total,
            tasks_ok     = s.ok,
            tasks_failed = s.failed,
            warn_count   = s.warn,
            block_count  = s.block,
            status = COALESCE(%(status)s,
                     CASE WHEN s.failed > 0 THEN 'FAILED'
                          WHEN s.total > 0 AND s.total = s.skipped THEN 'SKIPPED'
                          ELSE 'SUCCESS' END)
        FROM (
            SELECT count(*)                                  AS total,
                   count(*) FILTER (WHERE status = 'SUCCESS') AS ok,
                   count(*) FILTER (WHERE status = 'FAILED')  AS failed,
                   count(*) FILTER (WHERE status = 'SKIPPED') AS skipped,
                   coalesce(sum(warn_count), 0)              AS warn,
                   coalesce(sum(block_count), 0)             AS block
            FROM (SELECT DISTINCT ON (task_id) status, warn_count, block_count
                  FROM audit.task_run WHERE run_id = %(run_id)s
                  ORDER BY task_id, attempt DESC) latest
        ) s
        WHERE p.run_id = %(run_id)s""",
    "task_start": """
        INSERT INTO audit.task_run (run_id, task_id, attempt, pipeline, layer, entity, ref_month, status, started_at,
                                    airflow_task_id)
        VALUES (%(run_id)s, %(task_id)s, %(attempt)s, %(pipeline)s, %(layer)s, %(entity)s, %(ref_month)s, 'RUNNING',
                %(started_at)s, %(airflow_task_id)s)
        ON CONFLICT (run_id, task_id, attempt) DO UPDATE
            SET status = 'RUNNING', started_at = EXCLUDED.started_at, finished_at = NULL""",
    "task_fail_if_running": """
        UPDATE audit.task_run SET status = 'FAILED', finished_at = %(finished_at)s,
            exit_code = COALESCE(exit_code, -1),
            error_class = COALESCE(error_class, %(error_class)s),
            error_message = COALESCE(error_message, %(error_message)s)
        WHERE run_id = %(run_id)s AND airflow_task_id = %(airflow_task_id)s AND attempt = %(attempt)s
          AND status = 'RUNNING'""",
    "task_finish": """
        UPDATE audit.task_run SET
            ref_month = COALESCE(%(ref_month)s, ref_month),
            status = %(status)s, exit_code = %(exit_code)s, finished_at = %(finished_at)s,
            rows_in = %(rows_in)s, rows_out = %(rows_out)s, rows_rejected = %(rows_rejected)s,
            bytes_in = %(bytes_in)s, bytes_out = %(bytes_out)s,
            files_total = %(files_total)s, files_ok = %(files_ok)s, files_failed = %(files_failed)s,
            files_skipped = %(files_skipped)s,
            warn_count = %(warn_count)s, block_count = %(block_count)s, spark_app_id = %(spark_app_id)s,
            error_class = %(error_class)s, error_message = %(error_message)s, metrics = %(metrics)s
        WHERE run_id = %(run_id)s AND task_id = %(task_id)s AND attempt = %(attempt)s""",
    "dq_event": """
        INSERT INTO audit.dq_event (created_at, run_id, task_id, pipeline, ref_month, check_name, severity, status,
                                    observed, threshold, message, context)
        VALUES (%(created_at)s, %(run_id)s, %(task_id)s, %(pipeline)s, %(ref_month)s, %(check_name)s, %(severity)s,
                %(status)s, %(observed)s, %(threshold)s, %(message)s, %(context)s)""",
    "log_event": """
        INSERT INTO audit.log_event (ts, run_id, task_id, pipeline, level, logger, event, message, fields, raw_line)
        VALUES (%(ts)s, %(run_id)s, %(task_id)s, %(pipeline)s, %(level)s, %(logger)s, %(event)s, %(message)s,
                %(fields)s, %(raw_line)s)""",
    "health_check": """
        INSERT INTO audit.health_check (checked_at, run_id, task_id, pipeline, component, status, latency_ms, detail)
        VALUES (%(checked_at)s, %(run_id)s, %(task_id)s, %(pipeline)s, %(component)s, %(status)s, %(latency_ms)s,
                %(detail)s)""",
    "delta_table_stats": """
        INSERT INTO audit.delta_table_stats (captured_at, run_id, task_id, pipeline, layer, table_name, ref_month,
            operation, delta_version, num_files, size_bytes, rows_total, rows_written, rows_inserted, rows_updated,
            rows_deleted, files_added, files_removed, op_metrics)
        VALUES (%(captured_at)s, %(run_id)s, %(task_id)s, %(pipeline)s, %(layer)s, %(table_name)s, %(ref_month)s,
            %(operation)s, %(delta_version)s, %(num_files)s, %(size_bytes)s, %(rows_total)s, %(rows_written)s,
            %(rows_inserted)s, %(rows_updated)s, %(rows_deleted)s, %(files_added)s, %(files_removed)s,
            %(op_metrics)s)""",
    "quarantine_summary": """
        INSERT INTO audit.quarantine_summary (created_at, run_id, task_id, pipeline, ref_month, entity, file_name,
                                              reason, rows, repaired, sample)
        VALUES (%(created_at)s, %(run_id)s, %(task_id)s, %(pipeline)s, %(ref_month)s, %(entity)s, %(file_name)s,
                %(reason)s, %(rows)s, %(repaired)s, %(sample)s)""",
}


def utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)


class AuditSink:
    """One per process (see `get_sink`). Thread-safety is not needed: pipelines log from the main thread."""

    LOG_BATCH = 200
    LOG_FLUSH_S = 15.0
    URGENT_LEVELS = frozenset({"WARNING", "ERROR", "CRITICAL"})

    def __init__(self, enabled: bool | None = None, dsn: str | None = None, spool_dir: Path | None = None):
        s = get_settings()
        self.enabled = s.audit_enabled if enabled is None else enabled
        self.dsn = dsn or s.audit_dsn
        self.spool_dir = spool_dir or s.audit_spool_dir
        self._conn = None
        self._down = False
        self._log_buffer: list[dict[str, Any]] = []
        self._last_flush = time.monotonic()
        self._spool_file = self.spool_dir / f"{os.getpid()}-{datetime.now(UTC):%Y%m%dT%H%M%S}.jsonl"
        self.spooled = 0

    # ------------------------------------------------------------------ connection
    def _connection(self):
        if not self.enabled or self._down:
            return None
        if self._conn is not None and not self._conn.closed:
            return self._conn
        try:
            import psycopg  # local import: tests and audit-disabled runs never need the driver

            self._conn = psycopg.connect(self.dsn, autocommit=True)
            self._conn.execute("SELECT audit.ensure_log_partitions(2)")
            self._replay_spool()
            return self._conn
        except Exception as exc:  # noqa: BLE001 - observability must not break the pipeline
            self._down = True
            _log.warning("audit database unavailable, spooling events to %s: %s", self.spool_dir, exc)
            return None

    @staticmethod
    def _bind(params: dict[str, Any]) -> dict[str, Any]:
        from psycopg.types.json import Jsonb

        return {k: (Jsonb(v) if k in JSON_PARAMS and v is not None else v) for k, v in params.items()}

    # ------------------------------------------------------------------ write path
    def write(self, op: str, params: dict[str, Any]) -> None:
        if op not in SQL:
            raise KeyError(f"unknown audit operation: {op}")  # programming error: fail loudly in dev/tests
        if not self.enabled:
            return
        conn = self._connection()
        if conn is None:
            self._spool(op, params)
            return
        try:
            conn.execute(SQL[op], self._bind(params))
        except Exception as exc:  # noqa: BLE001
            _log.warning("audit write %s failed, spooled: %s", op, exc)
            self._spool(op, params)

    def write_many(self, op: str, rows: list[dict[str, Any]]) -> None:
        if not rows or not self.enabled:
            return
        conn = self._connection()
        if conn is None:
            for r in rows:
                self._spool(op, r)
            return
        try:
            with conn.cursor() as cur:
                cur.executemany(SQL[op], [self._bind(r) for r in rows])
        except Exception as exc:  # noqa: BLE001
            _log.warning("audit batch %s failed (%d rows), spooled: %s", op, len(rows), exc)
            for r in rows:
                self._spool(op, r)

    def log(self, row: dict[str, Any]) -> None:
        """Buffered, flushed when the batch is full, every LOG_FLUSH_S seconds, at once for WARNING+ (so a long task
        is visible in Grafana while it runs), and always at task end."""
        if not self.enabled:
            return
        self._log_buffer.append(row)
        if (len(self._log_buffer) >= self.LOG_BATCH or row.get("level") in self.URGENT_LEVELS
                or time.monotonic() - self._last_flush >= self.LOG_FLUSH_S):
            self.flush()

    def flush(self) -> None:
        rows, self._log_buffer = self._log_buffer, []
        self._last_flush = time.monotonic()
        self.write_many("log_event", rows)

    def close(self) -> None:
        self.flush()
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    # ------------------------------------------------------------------ spool
    def _spool(self, op: str, params: dict[str, Any]) -> None:
        try:
            self.spool_dir.mkdir(parents=True, exist_ok=True)
            with self._spool_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"op": op, "params": params}, default=_json_default, ensure_ascii=False) + "\n")
            self.spooled += 1
        except OSError as exc:
            _log.error("audit spool failed, event lost (%s): %s", op, exc)

    def _replay_spool(self) -> None:
        if not self.spool_dir.is_dir():
            return
        for path in sorted(self.spool_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime):
            if path == self._spool_file:
                continue
            try:
                lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
                with self._conn.transaction():
                    for item in lines:
                        self._conn.execute(SQL[item["op"]], self._bind(item["params"]))
                path.unlink()
                _log.info("audit spool replayed: %s (%d events)", path.name, len(lines))
            except Exception as exc:  # noqa: BLE001
                _log.warning("audit spool replay stopped at %s, kept for next time: %s", path.name, exc)
                return


_SINK: AuditSink | None = None


def get_sink() -> AuditSink:
    global _SINK
    if _SINK is None:
        _SINK = AuditSink()
    return _SINK


def reset_sink(sink: AuditSink | None = None) -> None:
    """Tests: replace or drop the process-wide sink."""
    global _SINK
    if _SINK is not None and _SINK is not sink:
        _SINK.close()
    _SINK = sink
