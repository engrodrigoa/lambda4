# ADR 0006 — ANP fuel prices: local landing, Postgres bronze with Polars, control in Postgres

**Status:** accepted (2026-10-22)

## Context

Second project of the lab: the ANP fuel price survey ("Série Histórica de Preços de Combustíveis e de GLP"), the full
history (the datalabs repo only loads the last weeks and the current year). Measured on the live page (2026-10-22):

* **224 data files, ~5.4 GB, ~40 M rows**: semesters since 2004 for automotive fuels (`ca`) and GLP, monthly files
  since 2023 (diesel-gnv, gasolina-etanol, glp), and a rolling "last 4 weeks" file per product.
* One 16-column layout everywhere (`;`, UTF-8 with or without BOM, CRLF). About 1 % of the records contain `;`
  inside a quoted field (valid CSV). Some semesters are ZIPs, one of them behind a `.csv` name (`ca-2014-01.csv`).
* Irregular monthly names (`precos-glp-12.csv`, `09-dados-abertos-precos-2026-09-glp.csv`, a typo `02-cados-...`),
  irregular GLP semester names (`precos-semestrais-glp2021-01.csv`), gaps on the page (ca 2022-S1, monthly 2026-04).
* gov.br resets connections under bursts of parallel requests; HEAD returns no size, a 1-byte ranged GET does.
* The data is small for this machine: one process with Polars reads ~520 k records/s; Postgres holds it easily on the
  SSD (bronze ~10–12 GB). The owner wants silver/gold in dbt (Postgres) and a Streamlit app on gold.

## Decision

1. **Landing on the local disk, not SeaweedFS.** `LANDING_URI` (default `/mnt/hdd1tb/lambda-dump/landing`) +
   `anp/<kind>/<product>/<label>__<sha256[:12]>.<csv|zip>`. The reader is a local Polars process and the target is
   Postgres: S3 would add an on-demand service with no consumer. The content hash in the name makes the archive
   immutable: a republished file lands next to the old version, so any version can be replayed. `s3://` stays a
   configuration change (`pipelines/commons/landing.py`).
2. **Control in Postgres (`ctrl.source_file` + `ctrl.source_file_event`), an exception to "ctrl = Delta in
   s3a://ctrl".** The bronze load updates its control row in the SAME transaction as the data, so a file is either
   loaded and marked or neither: idempotency guaranteed by the database instead of by ordering. Delta control would
   tie a Postgres-only pipeline to Spark and SeaweedFS. The tables are generic (column `pipeline`), and they hold
   only control: logs and audit stay in schema `audit`.
3. **File identity = `file_key`, independent of the publisher's name**: `semestral/ca/2014-S1`, `mensal/glp/2026-09`,
   `semanal/glp/2026-10-20` (weekly: the page's "atualizado em" date, because the URL never changes). Versions are
   counted by content (`sha256`).
4. **Change detection without downloading.** Discover probes the size (1-byte ranged GET) of downloaded files whose
   period ended in the last `ANP_RECHECK_DAYS` (120); `--full-recheck` probes all. A size change → PENDING again.
   A run with nothing new exits 99 and the DAG skips everything downstream.
5. **Bronze = `anp_bronze.precos_raw`, every column text, LIST-partitioned by `_source_kind`**, plus lineage
   (`_file_key`, `_file_sha256`, `_record_no`, `_run_id`, `_ingested_at`). Overlaps between semester, month and week
   are kept; silver resolves them. Reload of a file = `DELETE` by `_file_key` + `COPY`, in one transaction.
6. **Reader in two passes** (`pipelines/anp/reader.py`): Python's C `csv` reader counts fields per record first
   (Polars raises on extra fields but silently pads short records with nulls), then Polars reads clean UTF-8 files
   (fast path) or the frame is built from the good records and the bad ones go to `anp_bronze.precos_quarantine`.
   Encoding: UTF-8, then cp1252, then UTF-8 with replacement (WARN). Empty fields → NULL; nothing is trimmed.
7. **Severity** (`pipelines/commons/dq.py`): BLOCK = page with no known file, header without the contract columns,
   file without rows, rows written + quarantined ≠ records read. A refused file does not stop the others: it is marked
   FAILED and the BLOCK is raised after the batch. Everything else is WARN (unclassified links, period gaps, files
   changed or unlisted at the source, download/load failures retried next run, quarantine rate, dates outside the
   file's period, encoding fallback, extra columns).

## Consequences

* The first full run downloads ~5.5 GB sequentially (polite pause 1.5 s, `ANP_HTTP_PAUSE_S`), then loads ~40 M rows.
  Daily runs with nothing new end in seconds.
* The landing archive grows with every republished version (hash-named files are never overwritten): clean it by
  hand if ever needed; ctrl knows which version is current.
* Silver/gold are the owner's dbt project. The DAG publishes the Airflow asset `anp_bronze.precos_raw` to schedule it.
* Pieces made generic on the way: `pipelines/commons/{pg,file_ctrl,health,airflow_ops}.py`, `Fetcher.remote_size`.
