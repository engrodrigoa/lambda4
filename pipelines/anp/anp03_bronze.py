"""anp03 - bronze: downloaded ANP files -> anp_bronze.precos_raw (Postgres), as published, one transaction per file.

    python -m pipelines.anp.anp03_bronze
    python -m pipelines.anp.anp03_bronze --kinds semanal --limit 3

Behaviour
* Loads files that anp02 left DOWNLOADED with bronze PENDING/FAILED, oldest kind first (semestral, mensal, semanal).
* ZIPs are extracted to the SSD scratch (LAMBDA_SCRATCH_DIR/anp-extract) and the copy is deleted after the file is
  done. CSVs are read straight from the landing zone.
* Contract: the header must contain the 16 contract columns (else the file is refused: BLOCK at the end of the run,
  after the other files are loaded); extra columns are dropped (WARN). Every column is stored as text.
* Records with the wrong number of fields go to anp_bronze.precos_quarantine (never dropped silently) and are
  counted in audit.quarantine_summary.
* Idempotent: ONE transaction per file deletes the rows of that file_key (previous version included), COPYs the new
  rows and quarantine, and marks ctrl.source_file LOADED. Either everything is committed or nothing is.

Exit codes: 0 loaded (failed files are retried on the next run, WARN) | 99 nothing to load | 1 a BLOCK check failed
(contract columns missing, file without rows, rows written != rows read) or nothing could be loaded.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import Any

import polars as pl

from pipelines.anp.contract import COLUMNS, PIPELINE, SOURCE_KINDS
from pipelines.anp.reader import ReadResult, csv_path, read_records
from pipelines.commons import dq, file_ctrl, pg
from pipelines.commons.audit import get_sink, utcnow_iso
from pipelines.commons.file_ctrl import FileCtrl, PgFileCtrl
from pipelines.commons.logger import log_event
from pipelines.commons.settings import get_settings
from pipelines.commons.task import TaskRun, TaskSkipped, run_task

TASK_ID = "anp03_bronze"
TABLE = "anp_bronze.precos_raw"
QUARANTINE_TABLE = "anp_bronze.precos_quarantine"
PENDING = ("PENDING", "FAILED")
QUARANTINE_RATE_MAX = 0.001             # bronze.quarantine_rate threshold (share of records)
VERSION_DEVIATION_MAX = 0.20            # bronze.rows_vs_previous_version threshold
OUTSIDE_PERIOD_MAX = 0.01               # bronze.dates_outside_period threshold (share of rows)
PERIOD_SLACK = timedelta(days=7)        # collection weeks straddle month/semester borders
SAMPLE_LINES = 3


class FileRefused(Exception):
    """The file breaks the contract (BLOCK at the end of the run): missing columns or no rows."""

    def __init__(self, check: str, message: str):
        super().__init__(message)
        self.check = check


def pending_files(state: dict[str, dict[str, Any]], kinds: tuple[str, ...], limit: int) -> list[dict[str, Any]]:
    rows = [r for r in state.values()
            if r["download_status"] == "DOWNLOADED" and r["bronze_status"] in PENDING and r["source_kind"] in kinds]
    rows.sort(key=lambda r: (SOURCE_KINDS.index(r["source_kind"]), r["period_start"], r["file_key"]))
    return rows[:limit] if limit else rows


def bronze_frame(read: ReadResult, row: dict[str, Any], *, run_id: str, ingested_at: str) -> pl.DataFrame:
    """Contract columns in contract order (all text) + lineage columns."""
    return read.df.select(
        *[pl.col(c).cast(pl.Utf8) for c in COLUMNS],
        pl.lit(row["source_kind"]).alias("_source_kind"),
        pl.lit(row["file_key"]).alias("_file_key"),
        pl.lit(row["sha256"]).alias("_file_sha256"),
        pl.col("_record_no"),
        pl.lit(run_id).alias("_run_id"),
        pl.lit(ingested_at).alias("_ingested_at"),
    )


def quarantine_frame(read: ReadResult, row: dict[str, Any], *, run_id: str, at: str) -> pl.DataFrame:
    schema = {"_record_no": pl.Int64, "reason": pl.Utf8, "raw_record": pl.Utf8}
    q = pl.DataFrame(read.quarantine, schema=schema)
    return q.select(
        pl.lit(row["source_kind"]).alias("_source_kind"), pl.lit(row["file_key"]).alias("_file_key"),
        pl.lit(row["sha256"]).alias("_file_sha256"), "_record_no", "reason", "raw_record",
        pl.lit(run_id).alias("_run_id"), pl.lit(at).alias("_quarantined_at"),
    )


def profile(df: pl.DataFrame, row: dict[str, Any]) -> dict[str, Any]:
    """Cheap batch profile for the logs/audit: products, stations, date range, rows outside the file's period."""
    dates = df.select(pl.col("data_da_coleta").str.strptime(pl.Date, "%d/%m/%Y", strict=False).alias("d"))["d"]
    lo, hi = row["period_start"] - PERIOD_SLACK, row["period_end"] + PERIOD_SLACK
    outside = int(((dates < lo) | (dates > hi)).sum()) if df.height else 0
    by_product = df.group_by("produto").len().sort("produto")
    return {
        "rows_by_product": {str(p): n for p, n in by_product.iter_rows()},
        "stations": df["cnpj_da_revenda"].n_unique(),
        "ufs": df["estado_sigla"].n_unique(),
        "date_min": str(dates.min()) if df.height else None,
        "date_max": str(dates.max()) if df.height else None,
        "dates_unparsed": int(dates.null_count()),
        "dates_outside_period": outside,
    }


def load_file(task: TaskRun, conn, ctrl: FileCtrl, row: dict[str, Any], scratch: Path) -> dict[str, Any]:
    key = row["file_key"]
    t0 = time.monotonic()
    with csv_path(Path(row["local_path"]), row["file_format"], scratch, key.replace("/", "__")) as path:
        read = read_records(path)
        t_read = time.monotonic() - t0
        missing = [c for c in COLUMNS if c not in read.header]
        if missing:
            raise FileRefused("bronze.contract_columns", f"{key}: missing contract columns {missing}")
        extra = [c for c in read.header if c not in COLUMNS]
        if extra:
            task.check("bronze.extra_columns", False, message=f"{key}: columns outside the contract dropped {extra}",
                       file_key=key, extra=extra)
        if read.encoding != "utf-8-sig":
            task.check("bronze.encoding_fallback", False, message=f"{key}: decoded as {read.encoding}",
                       file_key=key, encoding=read.encoding)
        if read.df.height == 0:
            raise FileRefused("bronze.rows_nonzero", f"{key}: no data rows ({read.records} records read)")

        now = utcnow_iso()
        data = bronze_frame(read, row, run_id=task.ctx.run_id, ingested_at=now)
        quarantine = quarantine_frame(read, row, run_id=task.ctx.run_id, at=now)
        stats = profile(data, row)
        loaded = {**row, "bronze_status": "LOADED", "bronze_sha256": row["sha256"], "bronze_rows": data.height,
                  "bronze_quarantined": quarantine.height, "loaded_at": file_ctrl.now_utc(),
                  "last_run_id": task.ctx.run_id, "last_error": None}
        t1 = time.monotonic()
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TABLE} WHERE _source_kind = %s AND _file_key = %s", (row["source_kind"], key))
            replaced = cur.rowcount
            cur.execute(f"DELETE FROM {QUARANTINE_TABLE} WHERE _file_key = %s", (key,))
            written = pg.copy_frame(cur, TABLE, data)
            quarantined = pg.copy_frame(cur, QUARANTINE_TABLE, quarantine)
            # BLOCK inside the transaction: a mismatch rolls the file back
            task.check("bronze.rows_match_source", written + quarantined == read.records,
                       observed=written + quarantined, threshold=read.records,
                       message=f"{key}: rows written + quarantined vs records read", file_key=key)
            ctrl.save([loaded], cur=cur)
            ctrl.add_events([file_ctrl.event(PIPELINE, key, "LOADED", run_id=task.ctx.run_id, task_id=TASK_ID,
                                             version=row["version"], bytes=row["local_bytes"], sha256=row["sha256"],
                                             rows=written, detail={"quarantined": quarantined, "replaced": replaced,
                                                                   "encoding": read.encoding, "reader": read.path,
                                                                   **stats})], cur=cur)
        t_load = time.monotonic() - t1

    if quarantined:
        for reason, rows in Counter(q["reason"] for q in read.quarantine).items():
            sample = [q["raw_record"][:500] for q in read.quarantine if q["reason"] == reason][:SAMPLE_LINES]
            get_sink().write("quarantine_summary", {
                "created_at": utcnow_iso(), "run_id": task.ctx.run_id, "task_id": TASK_ID, "pipeline": PIPELINE,
                "ref_month": None, "entity": row["source_kind"], "file_name": key, "reason": f"parse:{reason}",
                "rows": rows, "repaired": False, "sample": "\n".join(sample)})
    rate = quarantined / read.records if read.records else 0.0
    task.check("bronze.quarantine_rate", rate <= QUARANTINE_RATE_MAX, observed=round(rate, 6),
               threshold=QUARANTINE_RATE_MAX, message=f"{key}: {quarantined} of {read.records} records quarantined",
               file_key=key)
    if read.records:
        share = stats["dates_outside_period"] / data.height
        task.check("bronze.dates_outside_period", share <= OUTSIDE_PERIOD_MAX, observed=round(share, 6),
                   threshold=OUTSIDE_PERIOD_MAX, file_key=key,
                   message=f"{key}: {stats['dates_outside_period']} rows dated outside "
                           f"{row['period_start']}..{row['period_end']} (+/- {PERIOD_SLACK.days} days)")
    if row["bronze_rows"]:
        deviation = abs(written - row["bronze_rows"]) / row["bronze_rows"]
        task.check("bronze.rows_vs_previous_version", deviation <= VERSION_DEVIATION_MAX, observed=round(deviation, 4),
                   threshold=VERSION_DEVIATION_MAX, file_key=key,
                   message=f"{key}: {written} rows vs {row['bronze_rows']} in the version it replaced")

    seconds = time.monotonic() - t0
    task.add(files_ok=1, rows_in=read.records, rows_out=written, rows_rejected=quarantined,
             bytes_in=row["local_bytes"] or 0)
    log_event(task.log, "file_loaded", f"{key}: {written:,} rows (+{quarantined} quarantined, {replaced:,} replaced) "
                                       f"in {seconds:.1f}s", file_key=key, rows=written, quarantined=quarantined,
              replaced=replaced, encoding=read.encoding, reader=read.path, read_s=round(t_read, 2),
              load_s=round(t_load, 2), seconds=round(seconds, 2),
              rows_per_s=round(written / seconds) if seconds else None, version=row["version"], **stats)
    return stats


def load_files(task: TaskRun, *, conn, ctrl: FileCtrl, scratch: Path, kinds: tuple[str, ...] = SOURCE_KINDS,
               limit: int = 0) -> None:
    todo = pending_files(ctrl.load(PIPELINE), kinds, limit)
    task.set(files_total=len(todo))
    if not todo:
        raise TaskSkipped("nothing to load")
    log_event(task.log, "bronze_start", f"{len(todo)} file(s) -> {TABLE}", files=len(todo), kinds=list(kinds),
              limit=limit)

    refused: dict[str, list[str]] = {}
    failed: list[str] = []
    for row in todo:
        key = row["file_key"]
        try:
            load_file(task, conn, ctrl, row, scratch)
        except Exception as exc:  # noqa: BLE001 - one bad file must not stop the others; recorded below
            error = f"{type(exc).__name__}: {exc}"[:500]
            ctrl.save([{**row, "bronze_status": "FAILED", "last_error": error, "last_run_id": task.ctx.run_id}])
            ctrl.add_events([file_ctrl.event(PIPELINE, key, "LOAD_FAILED", run_id=task.ctx.run_id, task_id=TASK_ID,
                                             version=row["version"], sha256=row["sha256"], detail={"error": error})])
            task.add(files_failed=1)
            log_event(task.log, "file_failed", f"{key}: {error}", level=logging.ERROR, file_key=key, error=error)
            if isinstance(exc, dq.BlockingCheckFailed):
                raise                                   # already recorded as BLOCK: the task stops here
            if isinstance(exc, FileRefused):
                refused.setdefault(exc.check, []).append(key)
            else:
                failed.append(key)

    loaded = task.counters["files_ok"] or 0
    if loaded:
        with conn.cursor() as cur:
            cur.execute(f"ANALYZE {TABLE}")
            cur.execute("SELECT c.relname, c.reltuples::bigint FROM pg_class c JOIN pg_namespace n "
                        "ON n.oid = c.relnamespace WHERE n.nspname = 'anp_bronze' AND c.relname LIKE 'precos_raw_%'")
            estimate = dict(cur.fetchall())
        task.metric(bronze_rows_estimate=estimate)
        log_event(task.log, "bronze_table_stats", f"{TABLE} ~{sum(estimate.values()):,} rows after ANALYZE",
                  **{f"rows_{k}": v for k, v in estimate.items()})

    task.metric(files_failed_keys=failed, files_refused=refused)
    task.check("bronze.load_failures", not failed, observed=len(failed), threshold=0,
               message=f"retried on the next run: {failed}" if failed else "no load failure")
    for check, keys in sorted(refused.items()):
        task.check(check, False, observed=len(keys), threshold=0, message=f"files refused: {keys}")
    if failed and not loaded:
        raise RuntimeError(f"no file could be loaded ({len(failed)} failed)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--kinds", default=",".join(SOURCE_KINDS),
                        help=f"comma separated subset of {','.join(SOURCE_KINDS)} (default: all)")
    parser.add_argument("--limit", type=int, default=0, help="load at most N files (0 = all)")
    args = parser.parse_args(argv)
    kinds = tuple(k.strip() for k in args.kinds.split(",") if k.strip())
    if not set(kinds) <= set(SOURCE_KINDS):
        parser.error(f"--kinds must be a subset of {SOURCE_KINDS}")

    def body(task: TaskRun) -> None:
        scratch = get_settings().scratch_dir / "anp-extract"
        with pg.connect() as conn:
            load_files(task, conn=conn, ctrl=PgFileCtrl(conn), scratch=scratch, kinds=kinds, limit=args.limit)

    return run_task(body, pipeline=PIPELINE, task_id=TASK_ID, layer="bronze", entity="precos_raw",
                    params={"kinds": list(kinds), "limit": args.limit})


if __name__ == "__main__":
    sys.exit(main())
