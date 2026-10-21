"""rfb02 - landing ZIPs -> record-aligned UTF-8 CSV chunks on the SSD scratch, with exact record counts.

    python -m pipelines.rfb.rfb02_extract --ref-month 2026-09 --entity estabelecimentos
    python -m pipelines.rfb.rfb02_extract --ref-month 2026-09 --entity domains      # the 6 small tables

Output (one directory per entity, recreated on every run):
    <scratch>/rfb-extract/ref=<YYYYMM>/<entity>/<Zipstem>-NNNN.csv     chunks, ~RFB_CHUNK_MB each, UTF-8
    <scratch>/rfb-extract/ref=<YYYYMM>/<entity>/_quarantine.jsonl      lines dropped by the record splitter
    <scratch>/rfb-extract/ref=<YYYYMM>/<entity>/_manifest.json         written LAST: "extract complete" marker

Why chunks: the CSVs need Spark's multiLine mode (quoted fields with newlines), which reads a file with ONE task.
Record-aligned chunks keep multiLine correct AND parallel. See pipelines/commons/csv_split.py.
No Spark here: this is pure streaming I/O, parallel across the ZIPs of the entity (multiprocessing).
The CRC of every ZIP member is verified by zipfile while streaming (a corrupt ZIP fails the task).
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

from pipelines.commons.csv_split import split_stream, write_manifest
from pipelines.commons.landing import landing_for
from pipelines.commons.logger import log_event
from pipelines.commons.refmonth import RefMonth
from pipelines.commons.settings import get_settings
from pipelines.commons.task import TaskRun, run_task
from pipelines.rfb.contract import COLUMNS, entity_of, expand_entities, files_of

PIPELINE = "rfb"
MIB = 1024 * 1024
GIB = 1024 * MIB
MANIFEST = "_manifest.json"
QUARANTINE = "_quarantine.jsonl"


def extract_root(ref: RefMonth) -> Path:
    return get_settings().scratch_dir / "rfb-extract" / f"ref={ref.yyyymm}"


def entity_dir(ref: RefMonth, entity: str) -> Path:
    return extract_root(ref) / entity


def _extract_one(zip_path: str, out_dir: str, target_bytes: int) -> dict:
    """Worker (separate process): stream one ZIP member into chunks. Returns plain data (picklable)."""
    t0 = time.monotonic()
    z_path = Path(zip_path)
    with zipfile.ZipFile(z_path) as z:
        members = z.infolist()
        if len(members) != 1:
            raise ValueError(f"{z_path.name}: expected 1 member, found {len(members)}")
        with z.open(members[0]) as raw:
            res = split_stream(raw, Path(out_dir), z_path.stem, source_name=z_path.name, target_bytes=target_bytes,
                               expected_fields=len(COLUMNS[entity_of(z_path.name)]))
    out = res.to_dict()
    out.update(member=members[0].filename, member_bytes=members[0].file_size, zip_bytes=z_path.stat().st_size,
               seconds=round(time.monotonic() - t0, 2))
    return out


def extract_entity(task: TaskRun, ref: RefMonth, entity: str, *, month_dir: Path, workers: int,
                   target_bytes: int) -> dict:
    log = task.log
    zips = [month_dir / f for f in files_of(entity)]
    missing = [z.name for z in zips if not z.is_file()]
    task.check("extract.source_files_present", not missing, observed=len(zips) - len(missing), threshold=len(zips),
               message=f"{entity}: " + ("all ZIPs present" if not missing else f"missing {missing}"), entity=entity)

    uncompressed = 0
    for z in zips:
        with zipfile.ZipFile(z) as zf:
            uncompressed += sum(i.file_size for i in zf.infolist())
    out_dir = entity_dir(ref, entity)
    if out_dir.exists():
        shutil.rmtree(out_dir)        # idempotent: a re-run never mixes chunks of two runs
    out_dir.mkdir(parents=True)
    free = shutil.disk_usage(out_dir).free
    task.check("extract.free_space", free >= uncompressed * 1.05, observed=round(free / GIB, 2),
               threshold=round(uncompressed * 1.05 / GIB, 2), message=f"{entity}: GiB free vs GiB needed",
               entity=entity)
    log_event(log, "extract_start", f"{entity}: {len(zips)} ZIPs, {uncompressed / GIB:.2f} GiB uncompressed",
              entity=entity, zips=len(zips), uncompressed_bytes=uncompressed, out_dir=str(out_dir), workers=workers)

    results: list[dict] = []
    t0 = time.monotonic()
    with ProcessPoolExecutor(max_workers=max(1, min(workers, len(zips)))) as pool:
        futures = {pool.submit(_extract_one, str(z), str(out_dir), target_bytes): z.name for z in zips}
        for fut in as_completed(futures):
            r = fut.result()          # a worker error (bad CRC, I/O) propagates and fails the task
            results.append(r)
            q = len(r["quarantined"])
            log_event(log, "zip_extracted", f"{r['source']}: {r['records']:,} records -> {len(r['chunks'])} chunks "
                      f"in {r['seconds']:.1f}s", entity=entity, file=r["source"], records=r["records"],
                      physical_lines=r["physical_lines"], multiline_records=r["multiline_records"],
                      chunks=len(r["chunks"]), bytes_in=r["bytes_in"], bytes_out=r["bytes_out"],
                      quarantined_lines=q, seconds=r["seconds"],
                      mib_per_s=round(r["bytes_in"] / MIB / r["seconds"], 1) if r["seconds"] else None)
    results.sort(key=lambda r: r["source"])
    elapsed = time.monotonic() - t0

    # quarantined lines (if any) go to a side file; bronze loads them into the Delta quarantine table
    quarantined = [{"source_file": r["source"], **q} for r in results for q in r["quarantined"]]
    with (out_dir / QUARANTINE).open("w", encoding="utf-8") as fq:
        for q in quarantined:
            fq.write(json.dumps(q, ensure_ascii=False) + "\n")

    totals = {k: sum(r[k] for r in results)
              for k in ("records", "physical_lines", "multiline_records", "bytes_in", "bytes_out")}
    manifest = {
        "pipeline": PIPELINE, "entity": entity, "ref_month": ref.yyyymm, "run_id": task.ctx.run_id,
        "created_at": datetime.now(UTC).isoformat(), "encoding": "utf-8", "columns": list(COLUMNS[entity]),
        "quarantined_lines": len(quarantined), **totals,
        "files": [{k: v for k, v in r.items() if k != "quarantined"} | {"quarantined_lines": len(r["quarantined"])}
                  for r in results],
    }
    write_manifest(out_dir / MANIFEST, manifest)

    task.check("extract.unbalanced_quote_lines", not quarantined, observed=len(quarantined), threshold=0,
               message=f"{entity}: physical lines quarantined by the record splitter", entity=entity)
    task.add(files_ok=len(results), rows_out=totals["records"], rows_rejected=len(quarantined),
             bytes_in=totals["bytes_in"], bytes_out=totals["bytes_out"])
    chunks = sum(len(r["chunks"]) for r in results)
    log_event(log, "extract_done", f"{entity}: {totals['records']:,} records, {chunks} chunks, "
              f"{totals['multiline_records']} multiline, {len(quarantined)} quarantined, {elapsed:.0f}s",
              entity=entity, chunks=chunks, seconds=round(elapsed, 1), **totals, quarantined_lines=len(quarantined),
              mib_per_s=round(totals["bytes_in"] / MIB / elapsed, 1) if elapsed else None)
    return manifest


def main(argv: list[str] | None = None) -> int:
    s = get_settings()
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--ref-month", required=True, help="YYYY-MM or YYYYMM")
    parser.add_argument("--entity", required=True, help="entity, 'domains' or 'all'")
    default_workers = str(min(6, os.cpu_count() or 2))
    parser.add_argument("--workers", type=int, default=int(s.get("RFB_EXTRACT_WORKERS", default_workers)))
    parser.add_argument("--chunk-mb", type=int, default=int(s.get("RFB_CHUNK_MB", "512")))
    args = parser.parse_args(argv)
    try:
        ref = RefMonth.parse(args.ref_month)
        entities = expand_entities(args.entity)
    except (ValueError, KeyError) as exc:
        parser.error(str(exc))

    def body(task: TaskRun) -> None:
        month_dir = landing_for().month_dir("rfb", ref, create=False)
        task.set(files_total=sum(len(files_of(e)) for e in entities))
        summary = {}
        for entity in entities:
            m = extract_entity(task, ref, entity, month_dir=month_dir, workers=args.workers,
                               target_bytes=args.chunk_mb * MIB)
            summary[entity] = {"records": m["records"], "chunks": sum(len(f["chunks"]) for f in m["files"]),
                               "quarantined_lines": m["quarantined_lines"]}
        task.metric(entities=summary, chunk_mb=args.chunk_mb, workers=args.workers)

    return run_task(body, pipeline=PIPELINE, task_id=f"rfb02_extract.{args.entity}", layer="extract", ref_month=ref,
                    entity=args.entity, params={"entity": args.entity, "chunk_mb": args.chunk_mb})


if __name__ == "__main__":
    sys.exit(main())
