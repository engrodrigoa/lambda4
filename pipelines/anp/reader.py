"""Read one ANP source file into a Polars frame of text columns, refusing bad records instead of guessing.

Two passes, the first one cheap:
1. Python's csv module (C reader) walks every record: decodes (UTF-8, then cp1252, then UTF-8 with replacement),
   counts fields per record and collects the records whose field count differs from the header.
2. Clean UTF-8 file -> Polars reads it (fast path). Otherwise the frame is built from the good records of pass 1
   and the bad ones go to quarantine.
Why not Polars alone: it raises on records with too many fields, but silently pads records with too FEW fields with
nulls, which would hide broken rows in bronze.

Empty fields become null on both paths. Values are never trimmed or converted here: that is silver's job.
"""
from __future__ import annotations

import contextlib
import csv
import shutil
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from pipelines.anp.contract import normalize_header

ENCODINGS = ("utf-8-sig", "cp1252")            # tried in order, strictly; then utf-8 with replacement
RAW_MAX_CHARS = 5000
csv.field_size_limit(10 * 1024 * 1024)


@dataclass
class ReadResult:
    df: pl.DataFrame                           # normalised source columns (all text) + _record_no
    header: list[str]                          # normalised header, in file order
    records: int                               # data records in the file (good + quarantined)
    encoding: str                              # utf-8-sig | cp1252 | utf-8-replace
    path: str                                  # fast (polars) | python
    quarantine: list[dict[str, Any]] = field(default_factory=list)   # {_record_no, reason, raw_record}


@contextlib.contextmanager
def csv_path(path: Path, fmt: str, scratch_dir: Path, name: str) -> Iterator[Path]:
    """The CSV to read: the landing file itself, or the single member of a ZIP extracted to the SSD scratch.
    The extracted copy is deleted on exit (after the bronze commit, or on failure)."""
    if fmt != "zip":
        yield path
        return
    scratch_dir.mkdir(parents=True, exist_ok=True)
    target = scratch_dir / f"{name}.csv"
    try:
        with zipfile.ZipFile(path) as z:
            member = next(m for m in z.infolist() if not m.is_dir())
            with z.open(member) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)
        yield target
    finally:
        target.unlink(missing_ok=True)


def _scan(path: Path, encoding: str, errors: str = "strict") -> tuple[list[str], int, list[dict[str, Any]]]:
    with path.open("r", encoding=encoding, errors=errors, newline="") as f:
        reader = csv.reader(f, delimiter=";", quotechar='"')
        header = [normalize_header(h) for h in next(reader, [])]
        width, records, bad = len(header), 0, []
        for records, row in enumerate(reader, start=1):
            if len(row) != width:
                bad.append({"_record_no": records, "reason": f"field_count_{len(row)}",
                            "raw_record": ";".join(row)[:RAW_MAX_CHARS]})
    return header, records, bad


def _python_frame(path: Path, encoding: str, errors: str, header: list[str], skip: set[int]) -> pl.DataFrame:
    rows, numbers = [], []
    with path.open("r", encoding=encoding, errors=errors, newline="") as f:
        reader = csv.reader(f, delimiter=";", quotechar='"')
        next(reader, None)
        for n, row in enumerate(reader, start=1):
            if n not in skip:
                rows.append([v if v != "" else None for v in row])
                numbers.append(n)
    df = pl.DataFrame(rows, schema=dict.fromkeys(header, pl.Utf8), orient="row")
    return df.with_columns(pl.Series("_record_no", numbers, dtype=pl.Int64))


def read_records(path: Path) -> ReadResult:
    scanned = None
    for enc in ENCODINGS:
        try:
            scanned, encoding, errors = _scan(path, enc), enc, "strict"
            break
        except UnicodeDecodeError:
            continue
    if scanned is None:
        scanned, encoding, errors = _scan(path, "utf-8-sig", "replace"), "utf-8-replace", "replace"
    header, records, bad = scanned

    if encoding == "utf-8-sig" and not bad and len(set(header)) == len(header):
        df = pl.read_csv(path, separator=";", quote_char='"', infer_schema=False, encoding="utf8",
                         has_header=True, new_columns=header)
        if df.height != records:
            raise ValueError(f"polars read {df.height} records, csv scan counted {records}: {path}")
        df = df.with_row_index("_record_no", offset=1).with_columns(pl.col("_record_no").cast(pl.Int64))
        return ReadResult(df=df, header=header, records=records, encoding=encoding, path="fast")

    if len(set(header)) != len(header):
        raise ValueError(f"duplicate column names in the header: {header}")
    df = _python_frame(path, "utf-8-sig" if encoding == "utf-8-replace" else encoding, errors, header,
                       {b["_record_no"] for b in bad})
    return ReadResult(df=df, header=header, records=records, encoding=encoding, path="python", quarantine=bad)
