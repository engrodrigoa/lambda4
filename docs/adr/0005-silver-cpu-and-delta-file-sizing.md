# ADR 0005 — Silver CPU cost, single-window dedup and Delta file sizing

**Status:** accepted (2026-10-21)

## Context

The first real run (`manual__2026-10-21T11:01:33.444773+00:00`, ref 2026-09, 3 h 33 min) had two hot spots, read from
the Spark event logs (History Server) and `audit.log_event`:

* **`estabelecimentos.silver` 53 min.** The job at `rfb04_silver.py:117` took 32 min; 25 of them in ONE stage (32
  tasks, ~270 s each, CPU-bound: 239 s CPU per task, GC < 2 %). Its plan is `scan → typing Project → partial
  HashAggregate` (the map side of `dropDuplicates()`): 73.4 M rows in, 73.4 M rows out (zero reduction), 30/32 tasks
  fell back to sort, 36 GB memory / 13 GB disk spill. The window sort after the shuffle (96 partitions, ~160 MB each,
  no coalescing) cost only 41 s of sort time: the spill was a symptom, not the cause.
  Measured on local Spark with synthetic bronze (13.2 M rows, same width, 6 cores): the **typing alone** costs
  73 µs/row of CPU (= 2.1 h for 73.4 M rows, matching the stage's 2.2 h "aggregation build" time, which includes the
  upstream Project). A JFR profile showed ~30 % in `java.util.regex` (4 `regexp_replace` per text column x 10 text
  columns, each converting UTF8String ↔ java.lang.String) and 51 % inside one huge generated method.
* **`maintenance` 69 min.** Delta's OPTIMIZE defaults are `minFileSize = maxFileSize = 1 GiB`: every file under 1 GiB
  is "small", and each 1 GiB bin is rewritten by ONE task. The silver tables had just been written as 96 files of
  ~55 MB (one per shuffle partition) and `gold.estabelecimento_atual` as 2688 files (96 tasks x 28 `uf` partitions):
  OPTIMIZE rewrote ~20 GB right after it was written (top 10 tables = 61 of the 69 min, bronze included).

## Decision

1. **Typing without per-step regexes** (`silver_model.clean_text`, `code`): `translate` maps SUB → `?`, CR/LF → space
   and deletes the other control chars in one pass; the single remaining regex (collapse whitespace runs) only runs
   when the value contains two spaces or a TAB; `code()` only runs its digits regex when the length differs from the
   width. Same output, verified row by row (0 differing rows on 300 k synthetic rows + 200 k adversarial values).
   Measured: **73.3 → 34.2 µs/row (−53 %)**.
2. **Dedup in one window** (`silver_model.dedup`): drop `dropDuplicates()`; partition by the natural key, order by
   `dedup_order` then by every other column (a total order: deterministic winner, type-1 columns included), flag a row
   equal to the previous one (`lag(struct) <=> struct`) as an exact duplicate. One shuffle + one sort instead of a
   hash aggregate over all columns + two shuffles. With it, `rfb04_silver` uses 192 shuffle partitions (was 96):
   ~80 MB per partition, so the window sort stops spilling.
   Measured locally (13.2 M synthetic rows, 6 cores, partitions scaled to the real volume; same winners checksum in
   every variant): old 338 s / 1869 CPU-s → typing only 236 s → typing + one window 240 s / 1163 CPU-s → **+ 192
   partitions 182 s / 1024 CPU-s, spill 9.9 → 1.4 GB** (3x partitions: 179 s, no gain). The local model is faithful:
   338 s × 73.4/13.2 ≈ 1880 s vs 1919 s for the real job.
3. **Right-sized files at write time**: Delta optimized writes in silver and gold sessions
   (`pipelines.commons.spark.DELTA_OPTIMIZED_WRITES`, target `DELTA_TARGET_FILE_MB = 256`, Databricks' auto-tuned
   size for tables < 2.56 TB; `binSize` = 3 x target because it counts lz4 shuffle bytes, ~3x the zstd parquet).
   Bronze keeps its ~100 MB files (one per CSV split), already fine. Measured locally (first-load MERGE, 13.16 M
   rows): 165 s → 5 files of 185 MB with optimized writes vs 169 s → 35 files of 27 MB without (+76 s of OPTIMIZE to
   fix them): the extra shuffle is free. `binSize` is in MiB.
4. **OPTIMIZE only where it pays** (`rfb06_maintenance --optimize auto`, default): a table is compacted when it has
   ≥ 8 files averaging < 64 MiB; OPTIMIZE then only rewrites files < 64 MiB into files ≤ 256 MiB
   (`spark.databricks.delta.optimize.minFileSize/maxFileSize`), 4x more bins in parallel than with 1 GiB bins.
   `--optimize always|never` for manual use. Each table logs the decision and its reason (`table_maintained`).
5. `spark.ui.showConsoleProgress=false` by default in `build_spark` (progress bars were 63 lines of `\r` noise in one
   task log, also shipped to Loki).

## Consequences

* Expected on the next run (same volume): `estabelecimentos.silver` 53 → ~38 min (dedup job 32 → ~17 min; the
  source cache and the MERGE, ~19 min, are unchanged; optimized writes add a shuffle), the other silver tasks shorter
  in proportion to their text columns, maintenance
  69 → ~10–15 min (silver/gold/bronze skip OPTIMIZE; VACUUM + stats remain ~20 s per table). Optimized writes add
  one shuffle to each silver/gold write (minutes, not tens of minutes).
* Rows still go through the same checks; `ranked` keeps its meaning (distinct rows with `_rn`).
* The OPTIMIZE rule uses the average file size from `DESCRIBE DETAIL` (no per-file listing): a table with a few big
  files and many tiny ones is only compacted once the tiny ones dominate. Monthly SCD2 MERGEs rewrite most files of a
  table anyway (closes and absences touch every file) and optimized writes keep those right-sized.
* Not done (follow-ups): deletion vectors for the monthly MERGE (would write only changed rows), `VACUUM ... LITE`
  (skip the full listing on SeaweedFS), avoiding the second 15 GB cache in `scd2.apply_month`.
