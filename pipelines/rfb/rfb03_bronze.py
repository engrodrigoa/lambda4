"""rfb03 - extracted CSV chunks -> Delta bronze (rfb_bronze.<entity>), reconciled against the extract counts.

    python -m pipelines.rfb.rfb03_bronze --ref-month 2026-09 --entity estabelecimentos
    python -m pipelines.rfb.rfb03_bronze --ref-month 2026-09 --entity domains

Bronze = the month exactly as the Receita published it: every contract column as STRING (no trim, no cast, no fix),
plus lineage columns (_source_file, _ingested_at, _run_id) and the partition column _ref_month (YYYYMM).

Reading: RFC 4180 (every field quoted, inner quotes doubled) with multiLine=true, because some quoted fields contain
newlines; each record-aligned chunk is one Spark task. mode=PERMISSIVE keeps a malformed row's raw text in
_corrupt_record instead of failing or silently shifting columns; those rows go to the quarantine (audit.quarantine).

Idempotent: the month is written with replaceWhere(_ref_month = M), so a re-run replaces exactly that partition.
Reconciliation (BLOCK): rows written + rows quarantined at parse == records counted by rfb02_extract.
The (expensive) second scan for malformed rows only runs when the counts do not already match.
Retention: keeps the newest RFB_BRONZE_RETENTION_MONTHS months, and never deletes a month the silver layer has not
applied yet (ctrl.layer_watermark silver.<entity> = DONE).
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import UTC, datetime

from pipelines.commons.ctrl import DeltaCtrl
from pipelines.commons.logger import log_event
from pipelines.commons.quarantine import Quarantine
from pipelines.commons.refmonth import RefMonth
from pipelines.commons.settings import get_settings
from pipelines.commons.spark import spark_session
from pipelines.commons.task import TaskRun, run_task
from pipelines.rfb.contract import COLUMNS, expand_entities
from pipelines.rfb.rfb02_extract import MANIFEST, QUARANTINE, entity_dir

PIPELINE = "rfb"
DATABASE = "rfb_bronze"
LINEAGE = [("_source_file", "string"), ("_ingested_at", "timestamp"), ("_run_id", "string"),
           ("_ref_month", "int")]
QUARANTINE_RATE_MAX = 0.001        # bronze.quarantine_rate threshold (0.1 %)
ROWS_DEVIATION_MAX = 0.10          # bronze.rows_vs_previous_month threshold (10 %)
CSV_OPTIONS = {
    "sep": ";", "quote": '"', "escape": '"', "multiLine": "true", "header": "false", "encoding": "UTF-8",
    "mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record",
    "ignoreLeadingWhiteSpace": "false", "ignoreTrailingWhiteSpace": "false",   # bronze keeps values as published
}
SPARK_CONF = {
    "spark.sql.parquet.compression.codec": "zstd",   # text-heavy columns: much smaller than snappy, scarce disk
    "spark.sql.shuffle.partitions": "24",
}


def table_name(entity: str) -> str:
    return f"{DATABASE}.{entity}"


def ensure_table(spark, entity: str, bucket: str) -> None:
    cols = ", ".join([f"{c} string" for c in COLUMNS[entity]] + [f"{c} {t}" for c, t in LINEAGE])
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE} COMMENT 'RFB CNPJ open data as published (all strings)' "
              f"LOCATION 's3a://{bucket}/rfb/'")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table_name(entity)} ({cols}) USING DELTA
                  PARTITIONED BY (_ref_month) LOCATION 's3a://{bucket}/rfb/{entity}'
                  COMMENT 'RFB {entity}: one partition per reference month, raw strings + lineage'""")


def read_chunks(spark, entity: str, paths: list[str]):
    from pyspark.sql import functions as F

    schema = ", ".join([f"{c} string" for c in COLUMNS[entity]] + ["_corrupt_record string"])
    df = spark.read.schema(schema).options(**CSV_OPTIONS).csv(paths)
    # chunk 'Estabelecimentos3-0002.csv' came from 'Estabelecimentos3.zip'
    source = F.concat(F.regexp_extract(F.col("_metadata.file_name"), r"^(.+)-\d{4}\.csv$", 1), F.lit(".zip"))
    return df.withColumn("_source_file", source)


def last_operation_metrics(spark, table: str) -> dict[str, int]:
    """operationMetrics of the latest commit (numOutputRows, numOutputBytes, numFiles...), via DESCRIBE HISTORY."""
    row = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select("operationMetrics", "version").first()
    metrics = {k: int(v) for k, v in (row["operationMetrics"] or {}).items() if str(v).isdigit()}
    metrics["version"] = int(row["version"])
    return metrics


def apply_retention(task: TaskRun, spark, ctrl, entity: str, keep_months: int) -> None:
    table = table_name(entity)
    months = sorted((r["_ref_month"] for r in spark.table(table).select("_ref_month").distinct().collect()),
                    reverse=True)
    deletable, protected = [], []
    for m in months[keep_months:]:
        wm = ctrl.get_watermark(PIPELINE, f"silver.{entity}", m)
        (deletable if wm and wm.get("status") == "DONE" else protected).append(m)
    if deletable:
        # integers only in the predicate (see the parameter-binding note in CLAUDE.md)
        spark.sql(f"DELETE FROM {table} WHERE _ref_month IN ({', '.join(str(int(m)) for m in deletable)})")
    log_event(task.log, "bronze_retention", f"{entity}: {len(months)} months in bronze, keep {keep_months}, "
              f"deleted {deletable or 'none'}, kept (silver not applied yet) {protected or 'none'}",
              entity=entity, months=months, deleted=deletable, protected=protected, keep_months=keep_months)


def load_entity(task: TaskRun, spark, ctrl: DeltaCtrl, quarantine: Quarantine, ref: RefMonth, entity: str) -> dict:
    from pyspark.sql import functions as F

    s = get_settings()
    log = task.log
    t0 = time.monotonic()
    layer = f"bronze.{entity}"
    table = table_name(entity)

    # ---------------------------------------------------------------- preconditions (BLOCK)
    src_dir = entity_dir(ref, entity)
    manifest_path = src_dir / MANIFEST
    task.check("bronze.extract_manifest_present", manifest_path.is_file(), entity=entity,
               message=f"{entity}: " + (str(manifest_path) if manifest_path.is_file()
                                        else f"run rfb02_extract first ({manifest_path} missing)"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    chunks = [c["path"] for f in manifest["files"] for c in f["chunks"]]
    expected = int(manifest["records"])

    ensure_table(spark, entity, s.bucket_bronze)
    started = datetime.now(UTC)
    ctrl.set_watermark({"pipeline": PIPELINE, "layer": layer, "ref_month": ref.yyyymm, "status": "RUNNING",
                        "rows": None, "run_id": task.ctx.run_id, "applied_at": None, "updated_at": started})
    log_event(log, "bronze_start", f"{entity}: {expected:,} records in {len(chunks)} chunks -> {table}",
              entity=entity, records=expected, chunks=len(chunks), table=table, csv_bytes=manifest["bytes_out"])

    try:
        # ------------------------------------------------------------ write the month (one pass)
        df = read_chunks(spark, entity, chunks)
        good = (df.where(F.col("_corrupt_record").isNull())
                .select(*COLUMNS[entity], "_source_file",
                        F.lit(started).cast("timestamp").alias("_ingested_at"),
                        F.lit(task.ctx.run_id).alias("_run_id"),
                        F.lit(ref.yyyymm).cast("int").alias("_ref_month")))
        t_write = time.monotonic()
        (good.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"_ref_month = {ref.yyyymm}")
         .saveAsTable(table))
        write_s = time.monotonic() - t_write
        om = last_operation_metrics(spark, table)
        written = om.get("numOutputRows", 0)
        log_event(log, "bronze_written", f"{entity}: {written:,} rows, {om.get('numFiles', 0)} files, "
                  f"{om.get('numOutputBytes', 0) / 1024 ** 2:.0f} MiB in {write_s:.0f}s (Delta v{om['version']})",
                  entity=entity, rows=written, files=om.get("numFiles"), bytes=om.get("numOutputBytes"),
                  seconds=round(write_s, 1), delta_version=om["version"],
                  rows_per_s=round(written / write_s) if write_s else None)

        # ------------------------------------------------------------ quarantine (extract + parse)
        extract_q = [json.loads(line) for line in (src_dir / QUARANTINE).read_text(encoding="utf-8").splitlines()
                     if line.strip()] if (src_dir / QUARANTINE).is_file() else []
        q_rows = [{"source_file": q["source_file"], "stage": "extract", "reason": q["reason"], "line_no": q["line"],
                   "raw_record": q["raw"]} for q in extract_q]
        parse_df, parse_count = None, 0
        if written != expected:
            # only now pay for a second scan: the rows Spark could not parse into the contract layout
            parse_df = (read_chunks(spark, entity, chunks).where(F.col("_corrupt_record").isNotNull())
                        .select("_source_file", F.lit("parse").alias("stage"),
                                F.lit("column_count_mismatch").alias("reason"),
                                F.lit(None).cast("bigint").alias("line_no"),
                                F.col("_corrupt_record").alias("raw_record"))
                        .withColumnRenamed("_source_file", "source_file"))
        quarantined = quarantine.replace(pipeline=PIPELINE, ref_month=ref.yyyymm, entity=entity, df=parse_df,
                                         rows=q_rows, run_id=task.ctx.run_id, task_id=task.task_id)
        parse_count = quarantined - len(q_rows)

        # ------------------------------------------------------------ checks
        task.check("bronze.rows_nonzero", written > 0, observed=written, threshold=1, entity=entity,
                   message=f"{entity}: rows written")
        task.check("bronze.rows_match_source", written + parse_count == expected, observed=written + parse_count,
                   threshold=expected, entity=entity, rows_written=written, quarantined_parse=parse_count,
                   message=f"{entity}: written {written:,} + quarantined {parse_count:,} vs extracted {expected:,}")
        rate = quarantined / max(expected + len(q_rows), 1)
        task.check("bronze.quarantine_rate", rate <= QUARANTINE_RATE_MAX, observed=round(rate, 6),
                   threshold=QUARANTINE_RATE_MAX, entity=entity, quarantined=quarantined,
                   message=f"{entity}: {quarantined:,} rows quarantined")
        prev = ctrl.get_watermark(PIPELINE, layer, ref.prev().yyyymm)
        if prev and prev.get("rows"):
            dev = abs(written - prev["rows"]) / prev["rows"]
            task.check("bronze.rows_vs_previous_month", dev <= ROWS_DEVIATION_MAX, observed=round(dev, 4),
                       threshold=ROWS_DEVIATION_MAX, entity=entity, rows=written, previous_rows=prev["rows"],
                       message=f"{entity}: rows vs {ref.prev()}")
    except Exception:
        ctrl.set_watermark({"pipeline": PIPELINE, "layer": layer, "ref_month": ref.yyyymm, "status": "FAILED",
                            "rows": None, "run_id": task.ctx.run_id, "applied_at": None,
                            "updated_at": datetime.now(UTC)})
        raise

    ctrl.set_watermark({"pipeline": PIPELINE, "layer": layer, "ref_month": ref.yyyymm, "status": "DONE",
                        "rows": written, "run_id": task.ctx.run_id, "applied_at": datetime.now(UTC),
                        "updated_at": datetime.now(UTC)})
    apply_retention(task, spark, ctrl, entity, int(s.get("RFB_BRONZE_RETENTION_MONTHS", "3")))

    task.add(rows_in=expected, rows_out=written, rows_rejected=quarantined, bytes_in=int(manifest["bytes_out"]),
             bytes_out=om.get("numOutputBytes", 0))
    if s.get("RFB_KEEP_EXTRACT", "0") not in ("1", "true"):
        freed = sum(f.stat().st_size for f in src_dir.iterdir() if f.is_file())
        shutil.rmtree(src_dir)
        log_event(log, "extract_cleaned", f"{entity}: removed {src_dir} ({freed / 1024 ** 3:.2f} GiB)",
                  entity=entity, path=str(src_dir), bytes=freed)
    elapsed = time.monotonic() - t0
    log_event(log, "bronze_done", f"{entity}: {written:,} rows, {quarantined} quarantined, {elapsed:.0f}s",
              entity=entity, rows=written, quarantined=quarantined, seconds=round(elapsed, 1),
              compression_ratio=round(manifest["bytes_out"] / om["numOutputBytes"], 2)
              if om.get("numOutputBytes") else None)
    return {"rows": written, "quarantined": quarantined, "files": om.get("numFiles"),
            "bytes": om.get("numOutputBytes"), "seconds": round(elapsed, 1), "delta_version": om["version"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--ref-month", required=True, help="YYYY-MM or YYYYMM")
    parser.add_argument("--entity", required=True, help="entity, 'domains' or 'all'")
    parser.add_argument("--master", default=None, help="Spark master (default: spark-defaults.conf, the cluster)")
    args = parser.parse_args(argv)
    try:
        ref = RefMonth.parse(args.ref_month)
        entities = expand_entities(args.entity)
    except (ValueError, KeyError) as exc:
        parser.error(str(exc))

    def body(task: TaskRun) -> None:
        s = get_settings()
        with spark_session(f"rfb03_bronze-{args.entity}", master=args.master, conf=SPARK_CONF) as spark:
            ctrl = DeltaCtrl(spark, bucket=s.bucket_ctrl).ensure()
            ref_row = ctrl.get_ref(PIPELINE, ref.yyyymm) or {}
            task.check("bronze.landing_complete", ref_row.get("status") == "DOWNLOADED",
                       message=f"ctrl.download_ref {ref}: {ref_row.get('status', 'absent')}")
            quarantine = Quarantine(spark, bucket=s.bucket_audit).ensure()
            summary = {e: load_entity(task, spark, ctrl, quarantine, ref, e) for e in entities}
            task.metric(entities=summary)

    return run_task(body, pipeline=PIPELINE, task_id=f"rfb03_bronze.{args.entity}", layer="bronze", ref_month=ref,
                    entity=args.entity, params={"entity": args.entity})


if __name__ == "__main__":
    sys.exit(main())
