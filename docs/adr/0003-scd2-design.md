# ADR 0003 — SCD2 history in silver with one Delta MERGE per month

**Status:** accepted (2026-10-21)

## Context

The owner wants to know *what changed, for which CNPJ, between which months*. The source publishes a full snapshot
every month (no change feed). Profile of 2026-09: estabelecimentos and simples have clean natural keys; empresas has
1 duplicated key; socios has no clean key (292 exact duplicates, a few conflicts).

## Decision

* Silver keeps one row per **version** of each natural key (SCD Type 2) with `valid_from`/`valid_to` as YYYYMM
  integers (the grain of the source), `is_current`, `change_type`, `closed_reason`.
* Change detection: `xxhash64` over the tracked columns, null-safe (NULL → `\u0000` token) and order-sensitive;
  compared only between versions of the same key, so 64 bits are enough (no global uniqueness needed).
* One atomic MERGE per month (staged-union pattern) handles new keys, changed keys, absent keys
  (`WHEN NOT MATCHED BY SOURCE`) and **Type 1** columns (contacts) updated in place without new versions.
* Dedup BEFORE the merge: exact duplicates dropped (WARN), conflicting duplicates resolved by a documented ordering,
  losers quarantined (WARN); after dedup the natural key must be unique (BLOCK).
* Months in order (BLOCK `silver.month_order`), from `ctrl.layer_watermark`; re-running the last month rolls it back
  first (delete versions born in M, re-open versions closed by M). The table stores its last applied month in the
  property `lambda.scd2.last_month` because a month without changes leaves no trace in the rows.
* Code lists (CNAE, municipalities...) are SCD1 snapshots.

## Consequences

* "As of month M" is a filter (`valid_from <= M AND (valid_to IS NULL OR valid_to >= M)`), so gold can rebuild any
  month; the gold change log is derived from consecutive versions (attribute-level diff).
* The first loaded month is a baseline: every key is NEW, and the change log skips "inclusions" for it.
* History before the first loaded month cannot be inserted later: backfills start from the oldest month (rebuild).
* Type 1 overwrites are not reverted by a rollback (by definition they keep no history).
* Full-table MERGE each month: cost grows with the history; liquid clustering / Z-order on `_key` are the next lever
  if MERGE time becomes a problem (measure first: `audit.task_run`, Spark UI).
