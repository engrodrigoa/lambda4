"""rfb01 - RFB reference month: public share -> landing zone (ZIPs), with control state in Delta (ctrl).

    python -m pipelines.rfb.rfb01_download --ref-month 2026-09
    python -m pipelines.rfb.rfb01_download --ref-month next        # resume: month after the last DOWNLOADED one

Behaviour
* Contract, not listing: exactly the 37 files of pipelines/rfb/contract.py (the source cannot be listed).
* A month whose 37 ZIPs are already readable on disk is not downloaded again and the source is not contacted.
* A file counts as done only if it is complete (Content-Length) and a readable ZIP; SHA-256 computed while streaming.
* ctrl.download_ref goes DOWNLOADING at start (crash marker) and ends DOWNLOADED / PARTIAL / NOT_PUBLISHED / FAILED;
  ctrl.download_file gets one row per file per run. Both are written even when the task fails.

Exit codes: 0 month complete | 99 month not published yet (skipped) | 1 failure (cause in the task_error event and in
ctrl.download_ref.last_error: source_auth_failed, source_dir_not_found, source_unreachable, source_incomplete).
"""
from __future__ import annotations

import argparse
import logging
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipelines.commons.ctrl import CtrlStore, DeltaCtrl, InMemoryCtrl
from pipelines.commons.http import Fetcher, sha256_file
from pipelines.commons.landing import landing_for
from pipelines.commons.logger import log_event
from pipelines.commons.refmonth import RefMonth
from pipelines.commons.settings import get_settings
from pipelines.commons.task import TaskRun, TaskSkipped, run_task
from pipelines.rfb import source
from pipelines.rfb.contract import EXPECTED_FILES

PIPELINE = "rfb"
TASK_ID = "rfb01_download"
GIB = 1024 ** 3
DEFAULT_MONTH_BYTES = 8 * GIB          # used for the free-space check until a previous month is known
SIZE_DEVIATION_MAX = 0.30              # landing.size_vs_previous_month threshold


def now_utc() -> datetime:
    return datetime.now(UTC)


def is_valid_zip(path: Path) -> bool:
    """Cheap integrity check: a truncated ZIP has no central directory and fails to open."""
    try:
        with zipfile.ZipFile(path) as z:
            return len(z.namelist()) > 0
    except (zipfile.BadZipFile, OSError):
        return False


def resolve_ref_month(arg: str | None, ctrl: CtrlStore) -> RefMonth:
    if arg and arg.strip().lower() != "next":
        return RefMonth.parse(arg)
    last = ctrl.last_downloaded(PIPELINE)
    if last:
        return RefMonth.parse(last).next()
    return RefMonth.parse(get_settings().get("RFB_START_MONTH", "2026-09"))


def download_month(task: TaskRun, ref: RefMonth, *, ctrl: CtrlStore, month_dir: Path, landing_uri: str,
                   fetcher: Fetcher, base_dir: str, free_bytes: int | None = None) -> None:
    log = task.log
    started = now_utc()
    prev = ctrl.get_ref(PIPELINE, ref.yyyymm) or {}
    prev_month = ctrl.get_ref(PIPELINE, ref.prev().yyyymm) or {}
    known = ctrl.latest_file_hashes(PIPELINE, ref.yyyymm)

    ref_row: dict[str, Any] = {
        "pipeline": PIPELINE, "ref_month": ref.yyyymm, "status": "DOWNLOADING",
        "files_expected": len(EXPECTED_FILES), "files_ok": prev.get("files_ok") or 0,
        "bytes_total": prev.get("bytes_total"), "landing_uri": landing_uri,
        "attempts": (prev.get("attempts") or 0) + 1,
        "first_attempt_at": prev.get("first_attempt_at") or started, "last_attempt_at": started,
        "downloaded_at": prev.get("downloaded_at"), "last_run_id": task.ctx.run_id, "last_error": None,
        "updated_at": started,
    }
    ctrl.upsert_ref(ref_row)
    task.set(files_total=len(EXPECTED_FILES))
    log_event(log, "month_start", f"{ref}: landing at {month_dir}", landing_dir=str(month_dir),
              attempt_of_month=ref_row["attempts"], previous_status=prev.get("status"))

    file_rows: list[dict[str, Any]] = []

    def file_row(name: str, status: str, **kw: Any) -> None:
        file_rows.append({"pipeline": PIPELINE, "ref_month": ref.yyyymm, "file_name": name, "status": status,
                          "run_id": task.ctx.run_id, "finished_at": now_utc(), **kw})

    def finish(status: str, error: str | None) -> None:
        """Persist control state; runs on success AND failure paths."""
        ok_rows = [r for r in file_rows if r["status"] in ("OK", "SKIPPED_EXISTING")]
        ref_row.update(status=status, last_error=error, updated_at=now_utc(), files_ok=len(ok_rows),
                       bytes_total=sum(r.get("local_bytes") or 0 for r in ok_rows) or ref_row["bytes_total"])
        if status == "DOWNLOADED":
            ref_row["downloaded_at"] = ref_row["downloaded_at"] or now_utc()
        ctrl.append_files(file_rows)
        ctrl.upsert_ref(ref_row)

    try:
        # ---------------------------------------------------------------- free space (WARN)
        present = {n for n in EXPECTED_FILES if is_valid_zip(month_dir / n)}
        present_bytes = sum((month_dir / n).stat().st_size for n in present)
        needed = max(0, int((prev_month.get("bytes_total") or DEFAULT_MONTH_BYTES) * 1.1) - present_bytes)
        if free_bytes is not None:
            task.check("landing.free_space", free_bytes >= needed, observed=round(free_bytes / GIB, 2),
                       threshold=round(needed / GIB, 2), message="GiB free vs GiB still needed for the month")

        # ---------------------------------------------------------------- source check (only if something is missing)
        missing = [n for n in EXPECTED_FILES if n not in present]
        if not missing:
            log_event(log, "month_already_local", f"{ref}: all {len(EXPECTED_FILES)} files already local and "
                                                  "readable, source not contacted")
        else:
            try:
                source.check_source(fetcher, base_dir, ref)
            except source.SourceError as exc:
                if exc.event == "source_month_not_published":
                    finish("NOT_PUBLISHED", exc.event)
                    task.check("landing.source_published", False, message=str(exc))
                    raise TaskSkipped(str(exc)) from exc
                finish("FAILED", exc.event)
                log_event(log, exc.event, str(exc), level=logging.ERROR)
                raise
            log_event(log, "source_ok", f"{ref}: published, {len(missing)} of {len(EXPECTED_FILES)} files to fetch",
                      share_dir=base_dir, to_fetch=len(missing))

        # ---------------------------------------------------------------- files
        for name in EXPECTED_FILES:
            dest = month_dir / name
            t_file = now_utc()
            if name in present:
                size = dest.stat().st_size
                cached = known.get(name)
                sha = cached[1] if cached and cached[0] == size else sha256_file(dest)
                file_row(name, "SKIPPED_EXISTING", local_bytes=size, expected_bytes=size, sha256=sha,
                         attempts=0, started_at=t_file)
                task.add(files_skipped=1)
                continue
            if dest.exists():
                log_event(log, "file_corrupt_local", f"{name}: local file is not a readable ZIP, downloading again",
                          level=logging.WARNING, file=name)
                dest.unlink()
            res = fetcher.download(source.file_url(base_dir, ref, name), dest, validate=is_valid_zip)
            if res.ok:
                mbps = res.bytes / (1024 * 1024) / res.seconds if res.seconds else None
                file_row(name, "OK", local_bytes=res.bytes, expected_bytes=res.expected_bytes, sha256=res.sha256,
                         http_status=res.status, attempts=res.attempts, started_at=t_file)
                task.add(bytes_out=res.bytes)
                log_event(log, "file_downloaded", f"{name}: {res.bytes / 1024 / 1024:.1f} MiB in {res.seconds:.1f}s",
                          file=name, bytes=res.bytes, seconds=round(res.seconds, 2),
                          mib_per_s=round(mbps, 2) if mbps else None, attempts=res.attempts,
                          sha256=res.sha256)
            else:
                status = "CORRUPT" if res.status == 200 else "MISSING"
                file_row(name, status, expected_bytes=res.expected_bytes, http_status=res.status,
                         attempts=res.attempts, started_at=t_file)
                task.add(files_failed=1)
                log_event(log, "file_failed", f"{name}: {res.reason}", level=logging.ERROR, file=name,
                          http_status=res.status, attempts=res.attempts, reason=res.reason)
            fetcher.pause()
    except TaskSkipped:
        raise
    except source.SourceError:
        raise
    except BaseException as exc:
        # unexpected failure or interruption mid-month: keep what we know, mark the month PARTIAL
        try:
            finish("PARTIAL", f"{type(exc).__name__}: {exc}"[:500])
        except Exception as ctrl_exc:  # noqa: BLE001 - never mask the original error
            log_event(log, "ctrl_write_failed", f"could not persist control state: {ctrl_exc}", level=logging.ERROR)
        raise

    # -------------------------------------------------------------------- outcome
    ok_rows = [r for r in file_rows if r["status"] in ("OK", "SKIPPED_EXISTING")]
    failed = sorted(r["file_name"] for r in file_rows if r["status"] not in ("OK", "SKIPPED_EXISTING"))
    total_bytes = sum(r["local_bytes"] or 0 for r in ok_rows)
    finish("DOWNLOADED" if not failed else "PARTIAL", None if not failed else "source_incomplete")
    task.set(files_ok=len(ok_rows))   # all valid files of the month (downloaded now + already local)
    downloaded_s = sum((r["finished_at"] - r["started_at"]).total_seconds() for r in file_rows if r["status"] == "OK")
    task.metric(month_bytes=total_bytes, month_gib=round(total_bytes / GIB, 3), download_seconds=round(downloaded_s, 1),
                files_missing=failed)

    if prev_month.get("bytes_total") and not failed:
        deviation = abs(total_bytes - prev_month["bytes_total"]) / prev_month["bytes_total"]
        task.check("landing.size_vs_previous_month", deviation <= SIZE_DEVIATION_MAX, observed=round(deviation, 4),
                   threshold=SIZE_DEVIATION_MAX, message=f"bytes vs {ref.prev()}",
                   bytes=total_bytes, previous_bytes=prev_month["bytes_total"])
    task.check("landing.contract_files_present", not failed, observed=len(ok_rows), threshold=len(EXPECTED_FILES),
               message=("all contract files present" if not failed
                        else f"{len(failed)} files did not come (re-run resumes): {failed}"))
    log_event(log, "month_done", f"{ref}: {len(ok_rows)}/{len(EXPECTED_FILES)} files, {total_bytes / GIB:.2f} GiB",
              bytes=total_bytes)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--ref-month", default=get_settings().get("RFB_REF_MONTH"),
                        help="YYYY-MM, YYYYMM or 'next' (default: env RFB_REF_MONTH, else next)")
    parser.add_argument("--ctrl", choices=("delta", "memory"), default="delta",
                        help="memory = no Spark, control state not persisted (debug only)")
    args = parser.parse_args(argv)

    try:
        explicit = RefMonth.parse(args.ref_month) if args.ref_month and args.ref_month.lower() != "next" else None
    except ValueError as exc:
        parser.error(str(exc))

    def body(task: TaskRun) -> None:
        landing = landing_for()
        if args.ctrl == "memory":
            _run(task, InMemoryCtrl(), landing)
            return
        from pipelines.commons.spark import spark_session

        s = get_settings()
        # control tables are tiny: a handful of shuffle partitions instead of the default avoids ~50 empty tasks
        with spark_session(TASK_ID, master=s.ctrl_spark_master, conf={"spark.sql.shuffle.partitions": "4"}) as spark:
            _run(task, DeltaCtrl(spark, bucket=s.bucket_ctrl).ensure(), landing)

    def _run(task: TaskRun, ctrl: CtrlStore, landing) -> None:
        ref = explicit or resolve_ref_month(args.ref_month, ctrl)
        task.set_ref_month(ref)
        token = source.share_token()
        download_month(task, ref, ctrl=ctrl, month_dir=landing.month_dir("rfb", ref), landing_uri=landing.uri,
                       fetcher=source.make_fetcher(token), base_dir=source.share_dir(),
                       free_bytes=landing.free_bytes())

    return run_task(body, pipeline=PIPELINE, task_id=TASK_ID, layer="landing", ref_month=explicit,
                    entity="all", params={"ctrl": args.ctrl, "ref_month_arg": args.ref_month})


if __name__ == "__main__":
    sys.exit(main())
