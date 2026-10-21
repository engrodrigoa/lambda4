"""Data-quality checks: the ONE place where a check's severity is decided.

BLOCK  = the failure would produce a wrong model: the task stops (exit 1).
WARN   = record it (audit.dq_event, log, metric, alert) and keep going.

A check must be registered here before it can be evaluated (`TaskRun.check` raises KeyError otherwise), so moving a
check from WARN to BLOCK is a one-line change, reviewed in git.
"""
from __future__ import annotations

from enum import StrEnum


class Severity(StrEnum):
    WARN = "WARN"
    BLOCK = "BLOCK"


SEVERITY: dict[str, Severity] = {
    # landing (downloads)
    "landing.source_published": Severity.WARN,          # month not published yet -> task skipped (exit 99)
    "landing.contract_files_present": Severity.BLOCK,   # bronze needs the whole month (37 files)
    "landing.free_space": Severity.WARN,                # landing disk may not fit the month
    "landing.size_vs_previous_month": Severity.WARN,    # total bytes deviate from the previous month
    # extract (zip -> chunked UTF-8 CSV on scratch)
    "extract.source_files_present": Severity.BLOCK,     # every ZIP of the entity is in the landing zone
    "extract.free_space": Severity.WARN,                # scratch may not fit the uncompressed entity
    "extract.unbalanced_quote_lines": Severity.WARN,    # physical lines quarantined by the record splitter
    # bronze
    "bronze.landing_complete": Severity.BLOCK,          # ctrl.download_ref says the month is DOWNLOADED
    "bronze.extract_manifest_present": Severity.BLOCK,  # extract finished for this entity and month
    "bronze.rows_nonzero": Severity.BLOCK,
    "bronze.rows_match_source": Severity.BLOCK,         # rows written + rows quarantined == records extracted
    "bronze.rows_vs_previous_month": Severity.WARN,
    "bronze.quarantine_rate": Severity.WARN,
    # silver
    "silver.month_order": Severity.BLOCK,               # SCD2 must apply months in order
    "silver.natural_key_unique": Severity.BLOCK,
    "silver.null_rate": Severity.WARN,
}


class BlockingCheckFailed(Exception):
    """A BLOCK check failed: the task must stop."""

    def __init__(self, check_name: str, message: str):
        super().__init__(f"{check_name}: {message}")
        self.check_name = check_name


def severity_of(check_name: str) -> Severity:
    try:
        return SEVERITY[check_name]
    except KeyError:
        raise KeyError(f"check '{check_name}' is not registered in pipelines/commons/dq.py SEVERITY") from None
