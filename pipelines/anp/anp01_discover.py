"""anp01 - discover: ANP portal page -> catalog of source files -> what needs downloading (ctrl.source_file).

    python -m pipelines.anp.anp01_discover                  # recheck sizes of recent files only
    python -m pipelines.anp.anp01_discover --full-recheck   # recheck the size of every downloaded file

Behaviour
* One GET of the page; every data link is classified by pipelines/anp/contract.py. Links the contract does not
  know are reported (scrape drift), never guessed.
* New files enter ctrl.source_file as PENDING (event DISCOVERED). Files no longer linked are marked unlisted.
* Change detection without downloading: a 1-byte ranged GET returns the size; a downloaded file whose size changed
  at the source goes back to PENDING (event CHANGED_AT_SOURCE). Only files whose period ended in the last
  ANP_RECHECK_DAYS (default 120) are rechecked on a normal run: old semesters do not change, and every request
  to gov.br counts (it throttles bursts).
* Nothing to download and nothing waiting for bronze -> exit 99 (skipped): no unnecessary run downstream.

Exit codes: 0 work pending | 99 up to date | 1 failure (page unavailable, or no file recognised at all).
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

from pipelines.anp import source
from pipelines.anp.contract import PIPELINE, SourceFile
from pipelines.commons import file_ctrl
from pipelines.commons.file_ctrl import FileCtrl, PgFileCtrl
from pipelines.commons.http import Fetcher
from pipelines.commons.logger import log_event
from pipelines.commons.settings import get_settings
from pipelines.commons.task import TaskRun, TaskSkipped, run_task

TASK_ID = "anp01_discover"
RETRY_DOWNLOAD = ("PENDING", "FAILED", "MISSING_AT_SOURCE")
RETRY_BRONZE = ("PENDING", "FAILED")


def needs_recheck(row: dict[str, Any], today: date, recheck_days: int, full: bool) -> bool:
    """Size probe only for downloaded, dated files (weekly ones are keyed by date) inside the recheck window."""
    if row["download_status"] != "DOWNLOADED" or row["source_kind"] == "semanal":
        return False
    return full or row["period_end"] >= today - timedelta(days=recheck_days)


def reconcile(existing: dict[str, dict[str, Any]], listed: list[SourceFile], *, now: datetime, run_id: str,
              remote_sizes: dict[str, int | None] | None = None) -> tuple[list[dict], list[dict], dict[str, list]]:
    """Pure merge of the page catalog into the control rows. Returns (rows to save, events, report)."""
    remote_sizes = remote_sizes or {}
    rows, events = [], []
    report: dict[str, list] = {"new": [], "changed": [], "unlisted": [], "relisted": []}

    def ev(key: str, name: str, **kw: Any) -> None:
        events.append(file_ctrl.event(PIPELINE, key, name, run_id=run_id, task_id=TASK_ID, **kw))

    listed_keys = set()
    for f in listed:
        listed_keys.add(f.file_key)
        meta = {"source_kind": f.source_kind, "product_group": f.product_group, "period_start": f.period_start,
                "period_end": f.period_end, "url": f.url, "link_text": f.link_text}
        row = existing.get(f.file_key)
        if row is None:
            rows.append(file_ctrl.new_row(pipeline=PIPELINE, file_key=f.file_key, **meta, first_seen_at=now,
                                          last_seen_at=now, last_run_id=run_id))
            ev(f.file_key, "DISCOVERED", detail={"url": f.url})
            report["new"].append(f.file_key)
            continue
        row = {**row, **meta, "last_seen_at": now, "last_run_id": run_id}
        if not existing[f.file_key]["listed"]:
            row["listed"] = True
            ev(f.file_key, "RELISTED", detail={"url": f.url})
            report["relisted"].append(f.file_key)
        if f.file_key in remote_sizes:
            size = remote_sizes[f.file_key]
            row.update(remote_bytes=size, remote_checked_at=now)
            if size is not None and row["local_bytes"] is not None and size != row["local_bytes"]:
                row["download_status"] = "PENDING"
                ev(f.file_key, "CHANGED_AT_SOURCE", version=row["version"], bytes=size,
                   detail={"local_bytes": row["local_bytes"], "remote_bytes": size})
                report["changed"].append(f.file_key)
        rows.append(row)

    for key, row in existing.items():
        if key not in listed_keys and row["listed"]:
            rows.append({**row, "listed": False, "last_run_id": run_id})
            ev(key, "UNLISTED")
            if row["source_kind"] != "semanal":       # last week's weekly key leaving the page is the normal cycle
                report["unlisted"].append(key)
    return rows, events, report


def period_gaps(files: list[SourceFile]) -> list[str]:
    """Missing periods inside each (kind, product) series of the page, e.g. ['mensal/glp/2026-04']."""
    series: dict[tuple[str, str], set[date]] = defaultdict(set)
    for f in files:
        if f.source_kind != "semanal":
            series[(f.source_kind, f.product_group)].add(f.period_start)
    gaps = []
    for (kind, group), starts in sorted(series.items()):
        step = 6 if kind == "semestral" else 1
        current, last = min(starts), max(starts)
        while current < last:
            month = current.month - 1 + step
            current = date(current.year + month // 12, month % 12 + 1, 1)
            if current not in starts:
                label = f"{current.year}-S{1 if current.month == 1 else 2}" if kind == "semestral" \
                    else f"{current.year}-{current.month:02d}"
                gaps.append(f"{kind}/{group}/{label}")
    return gaps


def discover(task: TaskRun, *, ctrl: FileCtrl, fetcher: Fetcher, html: str | None = None, full_recheck: bool = False,
             recheck_days: int = 120, today: date | None = None) -> dict[str, Any]:
    log, run_id = task.log, task.ctx.run_id
    now, today = file_ctrl.now_utc(), today or date.today()
    url = source.page_url()

    # ------------------------------------------------------------------------ page -> catalog
    if html is None:
        html = source.fetch_page(fetcher, url)
    catalog = source.parse_page(html, url)
    task.set(files_total=len(catalog.files))
    log_event(log, "catalog_parsed", f"{len(catalog.files)} files on the page, weekly date {catalog.weekly_date}",
              files=len(catalog.files), weekly_date=str(catalog.weekly_date), ignored=len(catalog.ignored),
              unclassified=len(catalog.unclassified), duplicates=len(catalog.duplicates), page_bytes=len(html))
    task.check("discover.catalog_nonempty", bool(catalog.files), observed=len(catalog.files), threshold=1,
               message="page parsed into known files" if catalog.files else f"no known file on {url}")
    task.check("discover.weekly_date_found", catalog.weekly_date is not None,
               message="'Quatro ultimas semanas (atualizado em dd/mm/aaaa)' found" if catalog.weekly_date
               else "weekly update date not found: weekly files not cataloged this run")
    drift = [f"{u} [{t}]" for u, t in catalog.unclassified] + [f"duplicate {k}: {u}" for k, u in catalog.duplicates]
    task.check("discover.unclassified_links", not drift, observed=len(drift), threshold=0,
               message=f"links the contract does not recognise: {drift[:10]}" if drift else "every data link known")
    gaps = period_gaps(catalog.files)
    task.check("discover.period_gaps", not gaps, observed=len(gaps), threshold=0,
               message=f"periods missing on the page: {gaps}" if gaps else "no gaps in the published series")

    # ------------------------------------------------------------------------ size probes (change detection)
    existing = ctrl.load(PIPELINE)
    to_probe = [f for f in catalog.files
                if f.file_key in existing and needs_recheck(existing[f.file_key], today, recheck_days, full_recheck)]
    sizes: dict[str, int | None] = {}
    for f in to_probe:
        status, size = fetcher.remote_size(f.url)
        sizes[f.file_key] = size
        if size is None:
            log_event(log, "size_probe_failed", f"{f.file_key}: no size (HTTP {status})", level=logging.WARNING,
                      file_key=f.file_key, http_status=status)
        fetcher.pause()
    task.metric(size_probes=len(to_probe), full_recheck=full_recheck, recheck_days=recheck_days)

    # ------------------------------------------------------------------------ merge + persist
    rows, events, report = reconcile(existing, catalog.files, now=now, run_id=run_id, remote_sizes=sizes)
    ctrl.save(rows)
    ctrl.add_events(events)
    task.check("discover.files_changed_at_source", not report["changed"], observed=len(report["changed"]),
               threshold=0, message=f"republished, downloading again: {report['changed']}" if report["changed"]
               else "no downloaded file changed at the source")
    task.check("discover.files_unlisted", not report["unlisted"], observed=len(report["unlisted"]), threshold=0,
               message=f"no longer on the page (kept in landing/bronze): {report['unlisted']}"
               if report["unlisted"] else "every known file still listed")

    state = ctrl.load(PIPELINE)
    pending_download = sorted(k for k, r in state.items()
                              if r["listed"] and r["download_status"] in RETRY_DOWNLOAD)
    pending_bronze = sorted(k for k, r in state.items()
                            if r["download_status"] == "DOWNLOADED" and r["bronze_status"] in RETRY_BRONZE)
    summary = {"new": len(report["new"]), "changed": len(report["changed"]), "unlisted": len(report["unlisted"]),
               "pending_download": len(pending_download), "pending_bronze": len(pending_bronze),
               "known": len(state)}
    task.metric(**summary)
    log_event(log, "discover_done", f"{summary['new']} new, {summary['changed']} changed, "
                                    f"{summary['pending_download']} to download, {summary['pending_bronze']} to load",
              **summary, pending_download_keys=pending_download[:50], pending_bronze_keys=pending_bronze[:50])
    if not pending_download and not pending_bronze:
        raise TaskSkipped("up to date: nothing to download and nothing waiting for bronze")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--full-recheck", action="store_true", help="probe the size of every downloaded file")
    args = parser.parse_args(argv)

    def body(task: TaskRun) -> None:
        from pipelines.commons import pg

        days = int(get_settings().get("ANP_RECHECK_DAYS", "120"))
        with pg.connect() as conn:
            discover(task, ctrl=PgFileCtrl(conn), fetcher=source.make_fetcher(), full_recheck=args.full_recheck,
                     recheck_days=days)

    return run_task(body, pipeline=PIPELINE, task_id=TASK_ID, layer="landing", entity="catalog",
                    params={"full_recheck": args.full_recheck})


if __name__ == "__main__":
    sys.exit(main())
