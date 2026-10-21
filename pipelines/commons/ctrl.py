"""Control state in Delta tables (database `ctrl`, bucket s3a://ctrl/). ONLY control, never logs.

* ctrl.download_ref       - current state per (pipeline, ref_month): status, files, bytes, where to resume.
                            Change Data Feed is on, so every status change is kept as history.
* ctrl.download_file      - append-only history, one row per file per download run (the download audit trail).
* ctrl.layer_watermark    - per (pipeline, layer, ref_month): which months each layer has applied (SCD2 needs the
                            last applied month to enforce ordering).

Tables are generic (column `pipeline`) so every project reuses them. One writer per table at a time (Delta on S3
without a commit coordinator): only download tasks write download_*, each layer task writes its own watermark rows.
`InMemoryCtrl` implements the same interface for tests and dry runs.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from pyspark.sql import Window
from pyspark.sql import functions as F

DOWNLOAD_REF = [
    ("pipeline", "string"), ("ref_month", "int"), ("status", "string"), ("files_expected", "int"),
    ("files_ok", "int"), ("bytes_total", "bigint"), ("landing_uri", "string"), ("attempts", "int"),
    ("first_attempt_at", "timestamp"), ("last_attempt_at", "timestamp"), ("downloaded_at", "timestamp"),
    ("last_run_id", "string"), ("last_error", "string"), ("updated_at", "timestamp"),
]
DOWNLOAD_FILE = [
    ("pipeline", "string"), ("ref_month", "int"), ("file_name", "string"), ("expected_bytes", "bigint"),
    ("local_bytes", "bigint"), ("sha256", "string"), ("http_status", "int"), ("attempts", "int"),
    ("status", "string"), ("started_at", "timestamp"), ("finished_at", "timestamp"), ("run_id", "string"),
]
LAYER_WATERMARK = [
    ("pipeline", "string"), ("layer", "string"), ("ref_month", "int"), ("status", "string"), ("rows", "bigint"),
    ("run_id", "string"), ("applied_at", "timestamp"), ("updated_at", "timestamp"),
]

REF_STATUSES = {"PENDING", "DOWNLOADING", "DOWNLOADED", "PARTIAL", "NOT_PUBLISHED", "FAILED"}
FILE_STATUSES = {"OK", "SKIPPED_EXISTING", "MISSING", "CORRUPT"}
WATERMARK_STATUSES = {"RUNNING", "DONE", "FAILED", "ROLLED_BACK"}


def _ddl(cols: list[tuple[str, str]]) -> str:
    return ", ".join(f"{name} {typ}" for name, typ in cols)


def _row(cols: list[tuple[str, str]], data: dict[str, Any]) -> tuple:
    unknown = set(data) - {c for c, _ in cols}
    if unknown:
        raise KeyError(f"unknown ctrl columns: {sorted(unknown)}")
    return tuple(data.get(c) for c, _ in cols)


class CtrlStore(Protocol):
    def get_ref(self, pipeline: str, ref_month: int) -> dict[str, Any] | None: ...
    def upsert_ref(self, row: dict[str, Any]) -> None: ...
    def append_files(self, rows: list[dict[str, Any]]) -> None: ...
    def latest_file_hashes(self, pipeline: str, ref_month: int) -> dict[str, tuple[int, str]]: ...
    def last_downloaded(self, pipeline: str) -> int | None: ...
    def set_watermark(self, row: dict[str, Any]) -> None: ...
    def get_watermark(self, pipeline: str, layer: str, ref_month: int) -> dict[str, Any] | None: ...
    def last_applied(self, pipeline: str, layer: str) -> int | None: ...


class DeltaCtrl:
    def __init__(self, spark, bucket: str = "ctrl", database: str = "ctrl"):
        self.spark = spark
        self.bucket = bucket
        self.db = database

    def _t(self, table: str):
        # DataFrame API on purpose: the Delta 4.1.0 SQL extension breaks spark.sql(..., args=...) parameter binding
        # on Spark 4.1 (UNBOUND_SQL_PARAMETER), and column expressions are injection-safe anyway.
        return self.spark.table(f"{self.db}.{table}")

    def _loc(self, table: str) -> str:
        return f"s3a://{self.bucket}/{table}"

    def ensure(self) -> DeltaCtrl:
        sql = self.spark.sql
        sql(f"CREATE DATABASE IF NOT EXISTS {self.db} COMMENT 'control state: download references, layer "
            f"watermarks (no logs)' LOCATION 's3a://{self.bucket}/'")
        sql(f"""CREATE TABLE IF NOT EXISTS {self.db}.download_ref ({_ddl(DOWNLOAD_REF)}) USING DELTA
                LOCATION '{self._loc("download_ref")}'
                COMMENT 'current download state per (pipeline, ref_month); CDF keeps every change'
                TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
        sql(f"""CREATE TABLE IF NOT EXISTS {self.db}.download_file ({_ddl(DOWNLOAD_FILE)}) USING DELTA
                LOCATION '{self._loc("download_file")}'
                COMMENT 'append-only: one row per file per download run'
                TBLPROPERTIES ('delta.appendOnly' = 'true')""")
        sql(f"""CREATE TABLE IF NOT EXISTS {self.db}.layer_watermark ({_ddl(LAYER_WATERMARK)}) USING DELTA
                LOCATION '{self._loc("layer_watermark")}'
                COMMENT 'which reference months each layer has applied'
                TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
        return self

    # ------------------------------------------------------------------ download_ref
    def get_ref(self, pipeline: str, ref_month: int) -> dict[str, Any] | None:
        rows = (self._t("download_ref")
                .where((F.col("pipeline") == pipeline) & (F.col("ref_month") == ref_month)).collect())
        return rows[0].asDict() if rows else None

    def upsert_ref(self, row: dict[str, Any]) -> None:
        if row.get("status") not in REF_STATUSES:
            raise ValueError(f"invalid download_ref status: {row.get('status')}")
        df = self.spark.createDataFrame([_row(DOWNLOAD_REF, row)], schema=_ddl(DOWNLOAD_REF))
        df.createOrReplaceTempView("_ctrl_ref_src")
        self.spark.sql(f"""MERGE INTO {self.db}.download_ref t USING _ctrl_ref_src s
                           ON t.pipeline = s.pipeline AND t.ref_month = s.ref_month
                           WHEN MATCHED THEN UPDATE SET *
                           WHEN NOT MATCHED THEN INSERT *""")

    def last_downloaded(self, pipeline: str) -> int | None:
        row = (self._t("download_ref")
               .where((F.col("pipeline") == pipeline) & (F.col("status") == "DOWNLOADED"))
               .agg(F.max("ref_month").alias("m")).first())
        return row["m"] if row else None

    # ------------------------------------------------------------------ download_file
    def append_files(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        bad = {r.get("status") for r in rows} - FILE_STATUSES
        if bad:
            raise ValueError(f"invalid download_file status: {bad}")
        df = self.spark.createDataFrame([_row(DOWNLOAD_FILE, r) for r in rows], schema=_ddl(DOWNLOAD_FILE))
        df.writeTo(f"{self.db}.download_file").append()

    def latest_file_hashes(self, pipeline: str, ref_month: int) -> dict[str, tuple[int, str]]:
        latest = Window.partitionBy("file_name").orderBy(F.col("finished_at").desc())
        rows = (self._t("download_file")
                .where((F.col("pipeline") == pipeline) & (F.col("ref_month") == ref_month)
                       & F.col("sha256").isNotNull() & F.col("status").isin("OK", "SKIPPED_EXISTING"))
                .withColumn("rn", F.row_number().over(latest)).where(F.col("rn") == 1)
                .select("file_name", "local_bytes", "sha256").collect())
        return {r["file_name"]: (r["local_bytes"], r["sha256"]) for r in rows}

    # ------------------------------------------------------------------ layer_watermark
    def set_watermark(self, row: dict[str, Any]) -> None:
        if row.get("status") not in WATERMARK_STATUSES:
            raise ValueError(f"invalid watermark status: {row.get('status')}")
        df = self.spark.createDataFrame([_row(LAYER_WATERMARK, row)], schema=_ddl(LAYER_WATERMARK))
        df.createOrReplaceTempView("_ctrl_wm_src")
        self.spark.sql(f"""MERGE INTO {self.db}.layer_watermark t USING _ctrl_wm_src s
                           ON t.pipeline = s.pipeline AND t.layer = s.layer AND t.ref_month = s.ref_month
                           WHEN MATCHED THEN UPDATE SET *
                           WHEN NOT MATCHED THEN INSERT *""")

    def get_watermark(self, pipeline: str, layer: str, ref_month: int) -> dict[str, Any] | None:
        rows = (self._t("layer_watermark")
                .where((F.col("pipeline") == pipeline) & (F.col("layer") == layer)
                       & (F.col("ref_month") == ref_month)).collect())
        return rows[0].asDict() if rows else None

    def last_applied(self, pipeline: str, layer: str) -> int | None:
        row = (self._t("layer_watermark")
               .where((F.col("pipeline") == pipeline) & (F.col("layer") == layer) & (F.col("status") == "DONE"))
               .agg(F.max("ref_month").alias("m")).first())
        return row["m"] if row else None


class InMemoryCtrl:
    """Same interface as DeltaCtrl, in memory: unit tests and --dry-run."""

    def __init__(self) -> None:
        self.refs: dict[tuple[str, int], dict[str, Any]] = {}
        self.files: list[dict[str, Any]] = []
        self.watermarks: dict[tuple[str, str, int], dict[str, Any]] = {}

    def get_ref(self, pipeline: str, ref_month: int) -> dict[str, Any] | None:
        r = self.refs.get((pipeline, ref_month))
        return dict(r) if r else None

    def upsert_ref(self, row: dict[str, Any]) -> None:
        if row.get("status") not in REF_STATUSES:
            raise ValueError(f"invalid download_ref status: {row.get('status')}")
        _row(DOWNLOAD_REF, row)
        self.refs[(row["pipeline"], row["ref_month"])] = {c: row.get(c) for c, _ in DOWNLOAD_REF}

    def last_downloaded(self, pipeline: str) -> int | None:
        months = [m for (p, m), r in self.refs.items() if p == pipeline and r["status"] == "DOWNLOADED"]
        return max(months) if months else None

    def append_files(self, rows: list[dict[str, Any]]) -> None:
        for r in rows:
            if r.get("status") not in FILE_STATUSES:
                raise ValueError(f"invalid download_file status: {r.get('status')}")
            _row(DOWNLOAD_FILE, r)
            self.files.append({c: r.get(c) for c, _ in DOWNLOAD_FILE})

    def latest_file_hashes(self, pipeline: str, ref_month: int) -> dict[str, tuple[int, str]]:
        out: dict[str, tuple[datetime, int, str]] = {}
        for r in self.files:
            if (r["pipeline"], r["ref_month"]) != (pipeline, ref_month) or not r["sha256"]:
                continue
            if r["status"] not in ("OK", "SKIPPED_EXISTING"):
                continue
            prev = out.get(r["file_name"])
            if prev is None or r["finished_at"] >= prev[0]:
                out[r["file_name"]] = (r["finished_at"], r["local_bytes"], r["sha256"])
        return {k: (v[1], v[2]) for k, v in out.items()}

    def set_watermark(self, row: dict[str, Any]) -> None:
        if row.get("status") not in WATERMARK_STATUSES:
            raise ValueError(f"invalid watermark status: {row.get('status')}")
        _row(LAYER_WATERMARK, row)
        self.watermarks[(row["pipeline"], row["layer"], row["ref_month"])] = dict(row)

    def get_watermark(self, pipeline: str, layer: str, ref_month: int) -> dict[str, Any] | None:
        r = self.watermarks.get((pipeline, layer, ref_month))
        return dict(r) if r else None

    def last_applied(self, pipeline: str, layer: str) -> int | None:
        months = [m for (p, lay, m), r in self.watermarks.items()
                  if p == pipeline and lay == layer and r["status"] == "DONE"]
        return max(months) if months else None
