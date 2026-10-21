"""Quarantine: rows a pipeline refused, kept in full (Delta, s3a://audit/quarantine) and counted for Grafana
(Postgres audit.quarantine_summary). Nothing is dropped silently.

Delta table `audit.quarantine` (generic across pipelines), partitioned by (pipeline, ref_month):
    pipeline, ref_month, entity, source_file, stage ('extract' | 'parse' | ...), reason, line_no, raw_record,
    run_id, quarantined_at
A re-run of the same (pipeline, ref_month, entity) REPLACES its previous quarantine rows (idempotent).
"""
from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from typing import Any

from pipelines.commons.audit import get_sink, utcnow_iso

COLUMNS = [
    ("pipeline", "string"), ("ref_month", "int"), ("entity", "string"), ("source_file", "string"),
    ("stage", "string"), ("reason", "string"), ("line_no", "bigint"), ("raw_record", "string"),
    ("run_id", "string"), ("quarantined_at", "timestamp"),
]
DDL = ", ".join(f"{c} {t}" for c, t in COLUMNS)
SAMPLE_LINES = 3


def _sql_str(value: str) -> str:
    """Values here come from code (contract names), never from data; still, never let a quote through."""
    if "'" in value or "\\" in value:
        raise ValueError(f"unexpected character in identifier value: {value!r}")
    return f"'{value}'"


class Quarantine:
    def __init__(self, spark, *, bucket: str = "audit", database: str = "audit"):
        from pipelines.commons.settings import get_settings

        self.spark = spark
        self.db = database
        self.table = f"{database}.quarantine"
        self.root = get_settings().lake_uri(bucket)
        self.location = get_settings().lake_uri(bucket, "quarantine")
        self.bucket = bucket

    def ensure(self) -> Quarantine:
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db} COMMENT 'audit data kept in the lake "
                       f"(quarantined rows); run/task logs live in Postgres schema audit' "
                       f"LOCATION '{self.root}/'")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.table} ({DDL}) USING DELTA
                           PARTITIONED BY (pipeline, ref_month) LOCATION '{self.location}'
                           COMMENT 'rows refused by pipelines, kept raw with the reason'""")
        return self

    def replace(self, *, pipeline: str, ref_month: int, entity: str, df=None, rows: list[dict[str, Any]] | None = None,
                run_id: str, task_id: str | None) -> int:
        """Delete the previous rows of (pipeline, ref_month, entity), append the new ones, write the summary.
        `df` must already have the quarantine columns except pipeline/ref_month/entity/run_id/quarantined_at."""
        from pyspark.sql import functions as F

        self.spark.sql(f"DELETE FROM {self.table} WHERE pipeline = {_sql_str(pipeline)} "
                       f"AND ref_month = {int(ref_month)} AND entity = {_sql_str(entity)}")
        frames = []
        if rows:
            cols = [c for c, _ in COLUMNS if c not in {"pipeline", "ref_month", "entity", "run_id", "quarantined_at"}]
            frames.append(self.spark.createDataFrame([tuple(r.get(c) for c in cols) for r in rows],
                                                     schema=", ".join(f"{c} {t}" for c, t in COLUMNS if c in cols)))
        if df is not None:
            frames.append(df)
        if not frames:
            return 0
        data = frames[0]
        for f in frames[1:]:
            data = data.unionByName(f)
        data = data.select(
            F.lit(pipeline).alias("pipeline"), F.lit(int(ref_month)).alias("ref_month"), F.lit(entity).alias("entity"),
            "source_file", "stage", "reason", F.col("line_no").cast("bigint"), "raw_record",
            F.lit(run_id).alias("run_id"), F.lit(datetime.now(UTC)).cast("timestamp").alias("quarantined_at"),
        ).persist()
        try:
            data.write.format("delta").mode("append").saveAsTable(self.table)
            summary = (data.groupBy("source_file", "stage", "reason")
                       .agg(F.count("*").alias("rows"),
                            F.slice(F.collect_list(F.substring("raw_record", 1, 500)), 1, SAMPLE_LINES)
                            .alias("sample"))
                       .collect())
        finally:
            data.unpersist()
        total = 0
        for s in summary:
            total += s["rows"]
            get_sink().write("quarantine_summary", {
                "created_at": utcnow_iso(), "run_id": run_id, "task_id": task_id, "pipeline": pipeline,
                "ref_month": ref_month, "entity": entity, "file_name": s["source_file"],
                "reason": f"{s['stage']}:{s['reason']}", "rows": s["rows"], "repaired": False,
                "sample": "\n".join(x for x in s["sample"] if x),
            })
        return total


def count_reasons(rows: list[dict[str, Any]]) -> dict[str, int]:
    return dict(Counter(r.get("reason") for r in rows))
