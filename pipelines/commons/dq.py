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
    # preflight (infra health before touching data)
    "preflight.s3": Severity.BLOCK,                     # SeaweedFS S3 answers with the lake credentials
    "preflight.spark_master": Severity.BLOCK,           # standalone master ALIVE with at least one worker
    "preflight.metastore": Severity.BLOCK,              # Hive metastore DB reachable (table names)
    "preflight.datalake_mounted": Severity.BLOCK,       # /mnt/datalake (SeaweedFS volumes) is a mountpoint
    "preflight.landing_mounted": Severity.BLOCK,        # landing disk mounted
    "preflight.free_space": Severity.WARN,              # scratch / datalake / landing above their floors
    "preflight.observability": Severity.WARN,           # Loki, Pushgateway, audit DB (degraded, not blocking)
    # silver
    "silver.bronze_month_present": Severity.BLOCK,      # bronze partition for the month exists (watermark DONE)
    "silver.month_order": Severity.BLOCK,               # SCD2 must apply months in order
    "silver.natural_key_unique": Severity.BLOCK,        # after dedup: one row per natural key
    "silver.duplicates_removed": Severity.WARN,         # exact duplicate rows in the source (dropped, counted)
    "silver.conflicting_duplicates": Severity.WARN,     # same key, different values (winner kept, rest quarantined)
    "silver.invalid_dates": Severity.WARN,              # date fields that could not be parsed (beyond zero dates)
    "silver.cnpj_dv_valid": Severity.WARN,              # CNPJ check digits that do not verify
    "silver.null_rate": Severity.WARN,
    "silver.change_rate": Severity.WARN,                # share of keys that changed this month (anomaly signal)
    "silver.current_rows_match_source": Severity.BLOCK, # current versions == rows of the month after dedup
    # gold
    "gold.rows_match_silver": Severity.BLOCK,           # fan-out / fan-in guard on joins
    "gold.orphan_rate": Severity.WARN,                  # foreign codes without a match in the dimension
    "gold.rows_nonzero": Severity.BLOCK,
    # maintenance
    "maintenance.vacuum_retention": Severity.WARN,      # never vacuum below the time-travel retention
    # discovery of scraped sources (ANP portal)
    "discover.catalog_nonempty": Severity.BLOCK,        # the page parsed into at least one known file
    "discover.unclassified_links": Severity.WARN,       # data links the contract does not recognise (scrape drift)
    "discover.weekly_date_found": Severity.WARN,        # "atualizado em" of the weekly files found on the page
    "discover.files_unlisted": Severity.WARN,           # files known before are no longer linked from the page
    "discover.files_changed_at_source": Severity.WARN,  # a downloaded file changed size at the source (republished)
    "discover.period_gaps": Severity.WARN,              # missing months/semesters inside a published series
    # downloads of scraped sources
    "landing.downloads_ok": Severity.BLOCK,             # files were pending and none could be downloaded
    "landing.download_failures": Severity.WARN,         # some files failed (retried on the next run)
    # bronze in Postgres (ANP)
    "bronze.contract_columns": Severity.BLOCK,          # header lacks contract columns (file refused, run fails)
    "bronze.extra_columns": Severity.WARN,              # header has columns outside the contract (dropped)
    "bronze.encoding_fallback": Severity.WARN,          # not valid UTF-8: decoded lossy (bad bytes replaced)
    "bronze.rows_vs_previous_version": Severity.WARN,   # a republished file differs too much from the loaded one
    "bronze.dates_outside_period": Severity.WARN,       # collection dates outside the file's period (misclassified?)
    "bronze.load_failures": Severity.WARN,              # some files failed to load (retried on the next run)
    "preflight.postgres": Severity.BLOCK,               # data database reachable and its schemas exist
    "preflight.source_reachable": Severity.WARN,        # source page answers (discover fails anyway if not)
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
