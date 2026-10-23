"""anp02 - download: pending files of ctrl.source_file -> landing zone (raw, immutable, versioned by content).

    python -m pipelines.anp.anp02_download
    python -m pipelines.anp.anp02_download --kinds semanal --limit 3      # a small first run, to practise

Behaviour
* Downloads only what anp01 left pending (PENDING / FAILED / MISSING_AT_SOURCE and still linked), sequentially with
  a polite pause (gov.br resets connections under bursts) and retries with backoff.
* A file counts as downloaded only if complete (Content-Length) and valid: a readable ZIP with one CSV inside, or a
  CSV whose first line is the ANP header. The format comes from magic bytes (some `.csv` links are ZIPs).
* Landing layout: <LANDING_URI>/anp/<kind>/<product>/<label>__<sha256[:12]>.<csv|zip>. The content hash in the name
  makes every version immutable: a republished file lands next to the old one, nothing is overwritten.
* Same content as the version already loaded -> no new version. Same content as ANOTHER file (e.g. a weekly file the
  source did not refresh) -> marked duplicate, bronze skips it.
* ctrl.source_file is saved after every file (a crash resumes where it stopped); ctrl.source_file_event keeps the
  history (DOWNLOADED, DOWNLOAD_FAILED, MISSING_AT_SOURCE, DUPLICATE_CONTENT).

Exit codes: 0 done (failed files are retried on the next run, WARN) | 99 nothing to download | 1 nothing could be
downloaded.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import zipfile
from pathlib import Path
from typing import Any

from pipelines.anp import source
from pipelines.anp.contract import PIPELINE, SOURCE_KINDS
from pipelines.commons import file_ctrl
from pipelines.commons.file_ctrl import FileCtrl, PgFileCtrl
from pipelines.commons.http import Fetcher
from pipelines.commons.landing import landing_for
from pipelines.commons.logger import log_event
from pipelines.commons.task import TaskRun, TaskSkipped, run_task

TASK_ID = "anp02_download"
MIB = 1024 ** 2
GIB = 1024 ** 3
DEFAULT_FILE_BYTES = 60 * MIB          # free-space estimate for files whose size is not known yet
ZIP_MAGIC = b"PK\x03\x04"
PENDING = ("PENDING", "FAILED", "MISSING_AT_SOURCE")


def file_format(path: Path) -> str:
    with path.open("rb") as f:
        return "zip" if f.read(4) == ZIP_MAGIC else "csv"


def _looks_like_header(first_line: bytes) -> bool:
    text = first_line.decode("utf-8", errors="replace").lower()
    return text.count(";") >= 10 and "sigla" in text and "produto" in text


def is_valid_source_file(path: Path) -> bool:
    """ZIP: readable, exactly one CSV member whose first line is the header. CSV: first line is the header."""
    try:
        if file_format(path) == "zip":
            with zipfile.ZipFile(path) as z:
                members = [m for m in z.infolist() if not m.is_dir()]
                if len(members) != 1 or not members[0].filename.lower().endswith(".csv"):
                    return False
                with z.open(members[0]) as f:
                    return _looks_like_header(f.readline(4096))
        with path.open("rb") as f:
            return _looks_like_header(f.readline(4096))
    except (zipfile.BadZipFile, OSError):
        return False


def pending_files(state: dict[str, dict[str, Any]], kinds: tuple[str, ...], limit: int) -> list[dict[str, Any]]:
    rows = [r for r in state.values()
            if r["listed"] and r["download_status"] in PENDING and r["source_kind"] in kinds]
    rows.sort(key=lambda r: (SOURCE_KINDS.index(r["source_kind"]), r["period_start"], r["file_key"]))
    return rows[:limit] if limit else rows


def download_files(task: TaskRun, *, ctrl: FileCtrl, fetcher: Fetcher, landing_root: Path,
                   kinds: tuple[str, ...] = SOURCE_KINDS, limit: int = 0, free_bytes: int | None = None) -> None:
    log, run_id = task.log, task.ctx.run_id
    state = ctrl.load(PIPELINE)
    todo = pending_files(state, kinds, limit)
    task.set(files_total=len(todo))
    if not todo:
        raise TaskSkipped("nothing to download")

    needed = int(sum(r["remote_bytes"] or DEFAULT_FILE_BYTES for r in todo) * 1.1)
    if free_bytes is not None:
        task.check("landing.free_space", free_bytes >= needed, observed=round(free_bytes / GIB, 2),
                   threshold=round(needed / GIB, 2), message="GiB free vs GiB estimated for the pending files")
    log_event(log, "download_start", f"{len(todo)} file(s) to download (~{needed / GIB:.2f} GiB) into {landing_root}",
              files=len(todo), estimated_bytes=needed, kinds=list(kinds), limit=limit)

    by_sha = {r["sha256"]: k for k, r in state.items() if r["sha256"] and r["download_status"] == "DOWNLOADED"}
    failed: list[str] = []
    for row in todo:
        key, label = row["file_key"], row["file_key"].rsplit("/", 1)[1]
        folder = landing_root / PIPELINE / row["source_kind"] / row["product_group"]
        folder.mkdir(parents=True, exist_ok=True)
        tmp = folder / f"{label}.download"
        res = fetcher.download(row["url"], tmp, validate=is_valid_source_file)
        row = {**row, "download_attempts": row["download_attempts"] + 1, "last_run_id": run_id}

        if not res.ok:
            missing = res.status in (404, 410)
            row.update(download_status="MISSING_AT_SOURCE" if missing else "FAILED", last_error=res.reason[:500])
            ctrl.save([row])
            ctrl.add_events([file_ctrl.event(PIPELINE, key, "MISSING_AT_SOURCE" if missing else "DOWNLOAD_FAILED",
                                             run_id=run_id, task_id=TASK_ID,
                                             detail={"http_status": res.status, "reason": res.reason,
                                                     "attempts": res.attempts, "url": row["url"]})])
            failed.append(key)
            task.add(files_failed=1)
            log_event(log, "file_failed", f"{key}: {res.reason}", level=logging.ERROR, file_key=key,
                      http_status=res.status, attempts=res.attempts, reason=res.reason, url=row["url"])
            fetcher.pause()
            continue

        fmt = file_format(tmp)
        dest = folder / f"{label}__{res.sha256[:12]}.{fmt}"
        if dest.exists():
            tmp.unlink()            # this exact content is already archived
        else:
            os.replace(tmp, dest)
        common = {"bytes": res.bytes, "sha256": res.sha256}
        mbps = round(res.bytes / MIB / res.seconds, 2) if res.seconds else None
        row.update(download_status="DOWNLOADED", local_path=str(dest), local_bytes=res.bytes, file_format=fmt,
                   remote_bytes=res.bytes, last_error=None)
        if res.sha256 == row["sha256"]:
            # same content as the version we have (e.g. a size probe false alarm): no new version, bronze untouched
            ctrl.save([row])
            ctrl.add_events([file_ctrl.event(PIPELINE, key, "DOWNLOADED", run_id=run_id, task_id=TASK_ID,
                                             version=row["version"], **common, detail={"unchanged": True})])
        elif res.sha256 in by_sha and by_sha[res.sha256] != key:
            twin = by_sha[res.sha256]
            row.update(sha256=res.sha256, version=row["version"] + 1, downloaded_at=file_ctrl.now_utc(),
                       duplicate_of=twin, bronze_status="SKIPPED_DUPLICATE")
            ctrl.save([row])
            ctrl.add_events([file_ctrl.event(PIPELINE, key, "DUPLICATE_CONTENT", run_id=run_id, task_id=TASK_ID,
                                             version=row["version"], **common, detail={"duplicate_of": twin})])
            log_event(log, "file_duplicate", f"{key}: same content as {twin}, not loaded twice",
                      level=logging.WARNING, file_key=key, duplicate_of=twin)
        else:
            row.update(sha256=res.sha256, version=row["version"] + 1, downloaded_at=file_ctrl.now_utc(),
                       duplicate_of=None, bronze_status="PENDING")
            ctrl.save([row])
            ctrl.add_events([file_ctrl.event(PIPELINE, key, "DOWNLOADED", run_id=run_id, task_id=TASK_ID,
                                             version=row["version"], **common,
                                             detail={"path": str(dest), "format": fmt, "seconds": round(res.seconds, 2),
                                                     "attempts": res.attempts})])
            by_sha[res.sha256] = key
        task.add(files_ok=1, bytes_out=res.bytes)
        log_event(log, "file_downloaded", f"{key}: {res.bytes / MIB:.1f} MiB {fmt} in {res.seconds:.1f}s",
                  file_key=key, bytes=res.bytes, seconds=round(res.seconds, 2), mib_per_s=mbps, format=fmt,
                  attempts=res.attempts, sha256=res.sha256, path=str(dest), version=row["version"])
        fetcher.pause()

    ok = task.counters["files_ok"] or 0
    task.metric(files_failed_keys=failed)
    task.check("landing.download_failures", not failed, observed=len(failed), threshold=0,
               message=f"retried on the next run: {failed}" if failed else "every pending file downloaded")
    task.check("landing.downloads_ok", ok > 0, observed=ok, threshold=1,
               message=f"{ok}/{len(todo)} downloaded")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--kinds", default=",".join(SOURCE_KINDS),
                        help=f"comma separated subset of {','.join(SOURCE_KINDS)} (default: all)")
    parser.add_argument("--limit", type=int, default=0, help="download at most N files (0 = all)")
    args = parser.parse_args(argv)
    kinds = tuple(k.strip() for k in args.kinds.split(",") if k.strip())
    if not set(kinds) <= set(SOURCE_KINDS):
        parser.error(f"--kinds must be a subset of {SOURCE_KINDS}")

    def body(task: TaskRun) -> None:
        from pipelines.commons import pg

        landing = landing_for()
        with pg.connect() as conn:
            download_files(task, ctrl=PgFileCtrl(conn), fetcher=source.make_fetcher(), landing_root=landing.root,
                           kinds=kinds, limit=args.limit, free_bytes=landing.free_bytes())

    return run_task(body, pipeline=PIPELINE, task_id=TASK_ID, layer="landing", entity="files",
                    params={"kinds": list(kinds), "limit": args.limit})


if __name__ == "__main__":
    sys.exit(main())
