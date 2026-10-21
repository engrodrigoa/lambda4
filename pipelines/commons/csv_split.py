"""Split a fully-quoted RFC 4180 CSV stream into UTF-8 chunks that always end on a record boundary.

Why: Spark must read these files with multiLine=true (some quoted fields contain newlines), and a multiLine file is
read by ONE task. Cutting a 6 GB file into ~512 MB chunks at record boundaries gives Spark back its parallelism while
keeping every record intact. Counting records here gives the exact number the bronze layer must reconcile with.

How a record boundary is found without parsing: when EVERY field is quoted and inner quotes are doubled (""), each
complete record has an even number of '"' characters. A physical line with an odd running count continues on the next
line (a newline inside quotes). This costs one bytes.count per line.

Guards against a stray, unescaped quote (which flips the parity):
* runaway: if a pending record grows beyond `max_span_lines` or `max_record_bytes`, its FIRST physical line
  (the culprit) is quarantined and the remaining buffered lines are re-scanned from a clean state;
* false pairing: a stray quote followed by the first line of a legitimate multiline record ALSO adds up to an even
  count, gluing several real records into one. So every multi-line candidate (rare: 7 in 73M rows) is validated with
  a real CSV parse (`validate`, e.g. "exactly N fields"); if it fails, the culprit line is quarantined and the rest
  re-scanned.
"""
from __future__ import annotations

import csv
import io
import json
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import BinaryIO

QUOTE = ord('"')


@dataclass
class Chunk:
    path: str
    records: int
    bytes: int


@dataclass
class SplitResult:
    source: str
    records: int = 0
    physical_lines: int = 0
    multiline_records: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    chunks: list[Chunk] = field(default_factory=list)
    quarantined: list[dict] = field(default_factory=list)   # {"line": n, "reason": ..., "raw": ...}

    def to_dict(self) -> dict:
        return asdict(self)


def iter_records(lines: Iterable[bytes], *, max_span_lines: int = 50, max_record_bytes: int = 1_000_000,
                 on_quarantine: Callable[[int, bytes, str], None] | None = None,
                 validate: Callable[[bytes], bool] | None = None) -> Iterator[tuple[bytes, int]]:
    """Yield (record_bytes, physical_line_count). Calls on_quarantine(line_no, raw, reason) for dropped lines."""
    source = enumerate(lines, start=1)
    rescan: deque[tuple[int, bytes]] = deque()     # lines to re-read after a culprit was dropped
    pending: list[tuple[int, bytes]] = []
    pending_bytes = quotes = 0
    exhausted = False

    def drop_culprit(reason: str) -> None:
        nonlocal pending, pending_bytes, quotes
        (n, culprit), rest = pending[0], pending[1:]
        if on_quarantine:
            on_quarantine(n, culprit, reason)
        rescan.extendleft(reversed(rest))           # keep original order, ahead of unread input
        pending, pending_bytes, quotes = [], 0, 0

    while True:
        if rescan:
            n, raw = rescan.popleft()
        elif not exhausted:
            try:
                n, raw = next(source)
            except StopIteration:
                exhausted = True
                continue
        elif pending:
            drop_culprit("unbalanced_quotes_at_eof")    # end of input inside an open quote
            continue
        else:
            return
        pending.append((n, raw))
        pending_bytes += len(raw)
        quotes += raw.count(QUOTE)
        if quotes % 2 == 0:
            record = b"".join(r for _, r in pending)
            if len(pending) > 1 and validate is not None and not validate(record):
                drop_culprit("unbalanced_quotes")       # false pairing: not one valid multiline record
                continue
            yield record, len(pending)
            pending, pending_bytes, quotes = [], 0, 0
        elif len(pending) > max_span_lines or pending_bytes > max_record_bytes:
            drop_culprit("unbalanced_quotes")


def field_count_validator(expected_fields: int, encoding: str = "iso-8859-1", delimiter: str = ";"):
    """A multiline candidate is valid only if it parses as ONE CSV row with exactly `expected_fields` fields."""
    def validate(record: bytes) -> bool:
        rows = list(csv.reader(io.StringIO(record.decode(encoding), newline=""), delimiter=delimiter, strict=True))
        return len(rows) == 1 and len(rows[0]) == expected_fields
    def safe(record: bytes) -> bool:
        try:
            return validate(record)
        except csv.Error:
            return False
    return safe


def split_stream(stream: BinaryIO, out_dir: Path, prefix: str, *, source_name: str, src_encoding: str = "iso-8859-1",
                 target_bytes: int = 512 * 1024 * 1024, max_span_lines: int = 50, max_record_bytes: int = 1_000_000,
                 expected_fields: int | None = None, quarantine_sample: int = 1000) -> SplitResult:
    """Write <out_dir>/<prefix>-NNNN.csv chunks (UTF-8, LF kept as in the source) and return the counts.
    `expected_fields` enables the false-pairing guard for multiline candidates (strongly recommended)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    res = SplitResult(source=source_name)
    part, chunk_records, chunk_bytes, fh = 0, 0, 0, None

    def quarantine(line: int, raw: bytes, reason: str) -> None:
        if len(res.quarantined) < quarantine_sample:
            res.quarantined.append({"line": line, "reason": reason,
                                    "raw": raw.decode(src_encoding, errors="replace")[:4000]})
        else:
            res.quarantined.append({"line": line, "reason": reason, "raw": None})

    def close_chunk() -> None:
        nonlocal fh, chunk_records, chunk_bytes
        if fh is not None:
            fh.close()
            res.chunks.append(Chunk(path=fh.name, records=chunk_records, bytes=chunk_bytes))
        fh, chunk_records, chunk_bytes = None, 0, 0

    def counted(lines: Iterable[bytes]) -> Iterator[bytes]:
        for raw in lines:
            res.physical_lines += 1
            res.bytes_in += len(raw)
            yield raw

    validate = field_count_validator(expected_fields, src_encoding) if expected_fields else None
    for record, span in iter_records(counted(stream), max_span_lines=max_span_lines,
                                     max_record_bytes=max_record_bytes, on_quarantine=quarantine, validate=validate):
        if not record.strip():
            continue                                    # blank line: not a record
        if fh is None:
            part += 1
            fh = (out_dir / f"{prefix}-{part:04d}.csv").open("wb")
        data = record.decode(src_encoding).encode("utf-8")
        if not data.endswith(b"\n"):
            data += b"\n"                               # last record of a file without a trailing newline
        fh.write(data)
        res.records += 1
        res.multiline_records += span > 1
        res.bytes_out += len(data)
        chunk_records += 1
        chunk_bytes += len(data)
        if chunk_bytes >= target_bytes:
            close_chunk()
    close_chunk()
    return res


def write_manifest(path: Path, payload: dict) -> None:
    """Atomic: the manifest is the 'extract finished' marker, so it must never be half-written."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    tmp.replace(path)
