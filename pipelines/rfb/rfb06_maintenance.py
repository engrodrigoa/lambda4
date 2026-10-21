"""rfb06 - Delta maintenance: OPTIMIZE (compact small files) + VACUUM (drop unreferenced files) + table stats.

    python -m pipelines.rfb.rfb06_maintenance --ref-month 2026-09

Why: every MERGE/overwrite leaves small files and old versions behind. OPTIMIZE rewrites small files into larger ones
(faster scans); VACUUM deletes files no version within the retention window still needs (disk is scarce here).
VACUUM limits time travel to the retention window: never go below 7 days (168 h), Delta's own safety default.
Each table's state after maintenance goes to audit.delta_table_stats (files, size, rows, version).
"""
from __future__ import annotations

import argparse
import sys
import time

from pipelines.commons.delta_stats import record_table_stats
from pipelines.commons.logger import log_event
from pipelines.commons.refmonth import RefMonth
from pipelines.commons.settings import get_settings
from pipelines.commons.spark import spark_session
from pipelines.commons.task import TaskRun, run_task

PIPELINE = "rfb"
DATABASES = {"rfb_bronze": "bronze", "rfb_silver": "silver", "rfb_gold": "gold", "ctrl": "ctrl", "audit": "audit"}
MIN_RETENTION_HOURS = 168
SPARK_CONF = {"spark.sql.parquet.compression.codec": "zstd"}


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
                    opt = spark.sql(f"OPTIMIZE {table}").select("metrics.numFilesAdded", "metrics.numFilesRemoved") \
                        .first()
                    if not args.skip_vacuum:
                        spark.sql(f"VACUUM {table} RETAIN {retention} HOURS")
                    stats = record_table_stats(task, spark, table, layer,
                                               ref_month=ref.yyyymm if ref else None)
                    secs = round(time.monotonic() - t0, 1)
                    summary[table] = {"files_before": before["numFiles"], "files_after": stats.get("num_files"),
                                      "bytes_before": before["sizeInBytes"], "bytes_after": stats.get("size_bytes"),
                                      "seconds": secs}
                    log_event(task.log, "table_maintained",
                              f"{table}: files {before['numFiles']} -> {stats.get('num_files')}, "
                              f"{(before['sizeInBytes'] or 0) / 1024 ** 2:.0f} -> "
                              f"{(stats.get('size_bytes') or 0) / 1024 ** 2:.0f} MiB in {secs}s",
                              table=table, layer=layer, files_added=opt[0], files_removed=opt[1],
                              **summary[table])          # summary carries `seconds` (a step span in the trace)
                    task.add(files_total=1, files_ok=1)
        task.metric(tables=summary, retention_hours=retention)

    return run_task(body, pipeline=PIPELINE, task_id="rfb06_maintenance", layer="other", ref_month=ref,
                    params={"retention_hours": args.retention_hours, "skip_vacuum": args.skip_vacuum})


if __name__ == "__main__":
    sys.exit(main())
