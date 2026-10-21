"""Delta table observability: what the last commit did and what the table looks like after it.

`record_table_stats(task, spark, table, layer)` reads
  * DESCRIBE HISTORY ... LIMIT 1  -> operation, version, operationMetrics (rows written/inserted/updated/deleted, files)
  * DESCRIBE DETAIL               -> numFiles, sizeInBytes of the whole table
  * count(*)                      -> answered from the Delta log statistics (no data scan)
and writes one row to audit.delta_table_stats plus a `table_stats` log event. Best effort: a failure here is logged as
a warning and never fails the task (observability must not break the pipeline).
"""
from __future__ import annotations

import logging
from typing import Any

from pipelines.commons.audit import get_sink, utcnow_iso
from pipelines.commons.logger import log_event

_INT_KEYS = {
    "rows_written": ("numOutputRows", "numTargetRowsInserted"),
    "rows_inserted": ("numTargetRowsInserted",),
    "rows_updated": ("numTargetRowsUpdated", "numUpdatedRows"),
    "rows_deleted": ("numTargetRowsDeleted", "numDeletedRows"),
    "files_added": ("numAddedFiles", "numFiles", "numTargetFilesAdded"),
    "files_removed": ("numRemovedFiles", "numTargetFilesRemoved"),
}


def _first(metrics: dict[str, int], keys: tuple[str, ...]) -> int | None:
    for k in keys:
        if k in metrics:
            return metrics[k]
    return None


def last_commit(spark, table: str) -> dict[str, Any]:
    row = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select("version", "operation", "operationMetrics").first()
    metrics = {k: int(v) for k, v in (row["operationMetrics"] or {}).items() if str(v).lstrip("-").isdigit()}
    return {"version": int(row["version"]), "operation": row["operation"], "metrics": metrics}


def table_detail(spark, table: str) -> dict[str, Any]:
    d = spark.sql(f"DESCRIBE DETAIL {table}").select("numFiles", "sizeInBytes", "location").first()
    return {"num_files": int(d["numFiles"] or 0), "size_bytes": int(d["sizeInBytes"] or 0), "location": d["location"]}


def record_table_stats(task, spark, table: str, layer: str, *, ref_month: int | None = None,
                       count_rows: bool = True) -> dict[str, Any]:
    try:
        commit = last_commit(spark, table)
        detail = table_detail(spark, table)
        rows_total = spark.table(table).count() if count_rows else None
        m = commit["metrics"]
        row = {
            "captured_at": utcnow_iso(), "run_id": task.ctx.run_id if task else None,
            "task_id": task.task_id if task else None, "pipeline": task.pipeline if task else "lambda",
            "layer": layer, "table_name": table, "ref_month": ref_month, "operation": commit["operation"],
            "delta_version": commit["version"], "num_files": detail["num_files"], "size_bytes": detail["size_bytes"],
            "rows_total": rows_total, **{k: _first(m, keys) for k, keys in _INT_KEYS.items()},
            "op_metrics": m,
        }
        get_sink().write("delta_table_stats", row)
        if task is not None:
            avg_mb = detail["size_bytes"] / detail["num_files"] / 1024 ** 2 if detail["num_files"] else 0
            rows_txt = f"{rows_total:,}" if rows_total is not None else "?"
            log_event(task.log, "table_stats",
                      f"{table} v{commit['version']} {commit['operation']}: {rows_txt} rows, "
                      f"{detail['num_files']} files, {detail['size_bytes'] / 1024 ** 2:.0f} MiB",
                      table=table, layer=layer, delta_version=commit["version"], operation=commit["operation"],
                      rows_total=rows_total, num_files=detail["num_files"], size_bytes=detail["size_bytes"],
                      avg_file_mib=round(avg_mb, 1), rows_written=row["rows_written"],
                      rows_inserted=row["rows_inserted"], rows_updated=row["rows_updated"])
        return row
    except Exception as exc:  # noqa: BLE001 - observability must not break the pipeline
        if task is not None:
            log_event(task.log, "table_stats_failed", f"{table}: could not collect stats: {exc}",
                      level=logging.WARNING, table=table)
        return {}
