"""rfb06 - Delta maintenance: OPTIMIZE (compact small files) + VACUUM (drop unreferenced files) + table stats.

    python -m pipelines.rfb.rfb06_maintenance --ref-month 2026-09

Why: every MERGE/overwrite leaves small files and old versions behind. OPTIMIZE rewrites small files into larger ones
(faster scans); VACUUM deletes files no version within the retention window still needs (disk is scarce here).
VACUUM limits time travel to the retention window: never go below 7 days (168 h), Delta's own safety default.
Each table's state after maintenance goes to audit.delta_table_stats (files, size, rows, version).

OPTIMIZE only where it pays (ADR 0005, `--optimize auto`): a table with at least OPTIMIZE_MIN_FILES files whose average
file is below SMALL_FILE_MB. Delta's defaults (minFileSize = maxFileSize = 1 GiB) call every file under 1 GiB "small"
and rewrite it with ONE core per 1 GiB bin: in 2026-09 that rewrote the fresh silver/gold tables (~50 of 69 min).
Here only files < SMALL_FILE_MB are compacted, into files of up to DELTA_TARGET_FILE_MB (4x more bins in parallel).
"""
from __future__ import annotations

import argparse
import sys
import time

from pipelines.commons.delta_stats import record_table_stats
from pipelines.commons.logger import log_event
from pipelines.commons.refmonth import RefMonth
from pipelines.commons.settings import get_settings
from pipelines.commons.spark import DELTA_TARGET_FILE_MB, spark_session
from pipelines.commons.task import TaskRun, run_task

PIPELINE = "rfb"
DATABASES = {"rfb_bronze": "bronze", "rfb_silver": "silver", "rfb_gold": "gold", "ctrl": "ctrl", "audit": "audit"}
MIN_RETENTION_HOURS = 168
OPTIMIZE_MIN_FILES = 8           # fewer files than this is never worth a rewrite (one job + one commit per table)
SMALL_FILE_MB = 64               # a file under 1/4 of the target is small; tables averaging below this get OPTIMIZE
MIB = 1024 ** 2
SPARK_CONF = {
    "spark.sql.parquet.compression.codec": "zstd",
    "spark.databricks.delta.optimize.minFileSize": str(SMALL_FILE_MB * MIB),          # bytes
    "spark.databricks.delta.optimize.maxFileSize": str(DELTA_TARGET_FILE_MB * MIB),   # bytes
}


def optimize_decision(num_files: int | None, size_bytes: int | None, mode: str = "auto") -> tuple[bool, str]:
    """(run OPTIMIZE?, why) from DESCRIBE DETAIL numbers; pure function (unit-tested)."""
    if mode in ("always", "never"):
        return mode == "always", f"--optimize {mode}"
    files, size = int(num_files or 0), int(size_bytes or 0)
    if files < OPTIMIZE_MIN_FILES:
        return False, f"{files} files < {OPTIMIZE_MIN_FILES}"
    avg_mb = size / files / MIB
    if avg_mb >= SMALL_FILE_MB:
        return False, f"{files} files averaging {avg_mb:.0f} MiB >= {SMALL_FILE_MB} MiB"
    return True, f"{files} files averaging {avg_mb:.1f} MiB < {SMALL_FILE_MB} MiB"


def delta_tables(spark, database: str) -> list[str]:
    if not spark.catalog.databaseExists(database):
        return []
    out = []
    for t in spark.catalog.listTables(database):
        if t.tableType != "VIEW" and not t.isTemporary:
            out.append(f"{database}.{t.name}")
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--ref-month", default=None, help="only tags the audit rows")
    parser.add_argument("--retention-hours", type=int,
                        default=int(get_settings().get("LAMBDA_VACUUM_RETENTION_HOURS", "168")))
    parser.add_argument("--skip-vacuum", action="store_true")
    parser.add_argument("--optimize", choices=("auto", "always", "never"), default="auto",
                        help="auto: only tables with many small files (default)")
    parser.add_argument("--master", default=None)
    args = parser.parse_args(argv)
    ref = RefMonth.parse(args.ref_month) if args.ref_month else None

    def body(task: TaskRun) -> None:
        retention = max(args.retention_hours, MIN_RETENTION_HOURS)
        task.check("maintenance.vacuum_retention", args.retention_hours >= MIN_RETENTION_HOURS,
                   observed=args.retention_hours, threshold=MIN_RETENTION_HOURS,
                   message=f"requested {args.retention_hours} h, using {retention} h")
        summary = {}
        with spark_session("rfb06_maintenance", master=args.master, conf=SPARK_CONF) as spark:
            for db, layer in DATABASES.items():
                for table in delta_tables(spark, db):
                    t0 = time.monotonic()
                    before = spark.sql(f"DESCRIBE DETAIL {table}").select("numFiles", "sizeInBytes").first()
                    optimize, why = optimize_decision(before["numFiles"], before["sizeInBytes"], args.optimize)
                    opt = (0, 0)
                    if optimize:
                        opt = spark.sql(f"OPTIMIZE {table}") \
                            .select("metrics.numFilesAdded", "metrics.numFilesRemoved").first()
                    if not args.skip_vacuum:
                        spark.sql(f"VACUUM {table} RETAIN {retention} HOURS")
                    stats = record_table_stats(task, spark, table, layer,
                                               ref_month=ref.yyyymm if ref else None)
                    secs = round(time.monotonic() - t0, 1)
                    summary[table] = {"files_before": before["numFiles"], "files_after": stats.get("num_files"),
                                      "bytes_before": before["sizeInBytes"], "bytes_after": stats.get("size_bytes"),
                                      "optimized": optimize, "seconds": secs}
                    log_event(task.log, "table_maintained",
                              f"{table}: {'OPTIMIZE' if optimize else 'no OPTIMIZE'} ({why}), files "
                              f"{before['numFiles']} -> {stats.get('num_files')}, "
                              f"{(before['sizeInBytes'] or 0) / MIB:.0f} -> "
                              f"{(stats.get('size_bytes') or 0) / MIB:.0f} MiB in {secs}s",
                              table=table, layer=layer, files_added=opt[0], files_removed=opt[1], optimize_reason=why,
                              **summary[table])          # summary carries `seconds` (a step span in the trace)
                    task.add(files_total=1, files_ok=1)
        task.metric(tables=summary, retention_hours=retention, optimize_mode=args.optimize,
                    tables_optimized=sum(1 for t in summary.values() if t["optimized"]))

    return run_task(body, pipeline=PIPELINE, task_id="rfb06_maintenance", layer="other", ref_month=ref,
                    params={"retention_hours": args.retention_hours, "skip_vacuum": args.skip_vacuum,
                            "optimize": args.optimize})


if __name__ == "__main__":
    sys.exit(main())
