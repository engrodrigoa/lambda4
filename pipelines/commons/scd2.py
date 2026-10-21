"""Generic SCD Type 2 on Delta Lake with a single MERGE per month (Spark SQL), plus Type 1 columns and rollback.

Table layout (one row per VERSION of a natural key):
    <key cols> <tracked cols> <type-1 cols>
    _key          string   natural key as one string (key cols joined by '|'): the MERGE join column
    row_hash      bigint   xxhash64 of the tracked cols  -> a different hash = a new version
    t1_hash       bigint   xxhash64 of the type-1 cols   -> a different hash = overwrite in place, no new version
    valid_from    int      first reference month (YYYYMM) of this version
    valid_to      int      last reference month of this version; NULL while current
    is_current    boolean
    change_type   string   NEW (first time the key is seen) | CHANGED (a tracked attribute changed)
    closed_reason string   CHANGED | ABSENT (the key disappeared from the source) ; NULL while current
    _run_id, _updated_at   lineage

One month M is applied with ONE atomic MERGE (the classic "staged union" pattern):
    staged = every source row with merge_key = _key          -> matches the current version of its key
           + changed rows again with merge_key = NULL       -> never matches -> inserted as the new version
    MERGE ... ON t._key = s.merge_key AND t.is_current
      WHEN MATCHED AND hash differs            -> close the current version (valid_to = M-1)
      WHEN MATCHED AND only type-1 hash differs-> update type-1 columns in place
      WHEN NOT MATCHED                         -> insert (new key, or new version of a changed key)
      WHEN NOT MATCHED BY SOURCE AND current   -> close: the key is absent from month M

Rules the caller must respect (enforced by rfb04_silver with ctrl watermarks):
* months are applied in order; the source has exactly one row per key (dedup BEFORE apply);
* re-applying the LAST month first calls rollback(M), which deletes the versions born in M and re-opens the versions
  M closed. Type-1 overwrites made by M are not reverted (type 1 keeps no history, by definition).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from pipelines.commons.refmonth import RefMonth

META_COLUMNS = [
    ("_key", "string"), ("row_hash", "bigint"), ("t1_hash", "bigint"), ("valid_from", "int"), ("valid_to", "int"),
    ("is_current", "boolean"), ("change_type", "string"), ("closed_reason", "string"), ("_run_id", "string"),
    ("_updated_at", "timestamp"),
]
_SAFE = re.compile(r"^[A-Za-z0-9_.:+\-]+$")
NULL_TOKEN = "\u0000"     # null-safe hashing: (NULL, 'a') and ('a', NULL) must not collide


@dataclass(frozen=True)
class Scd2Spec:
    table: str                              # e.g. rfb_silver.estabelecimento_hist
    location: str                           # e.g. s3a://silver/rfb/estabelecimento_hist
    key_cols: tuple[str, ...]
    tracked_cols: tuple[str, ...]
    type1_cols: tuple[str, ...] = field(default_factory=tuple)
    comment: str = ""

    @property
    def data_cols(self) -> tuple[str, ...]:
        return (*self.key_cols, *self.tracked_cols, *self.type1_cols)


def _lit(value: str) -> str:
    if not _SAFE.match(value):
        raise ValueError(f"unsafe value for a SQL literal: {value!r}")
    return f"'{value}'"


def hash_expr(cols: tuple[str, ...] | list[str]):
    """xxhash64 over the columns, null-safe and order-sensitive (each value is hashed separately by Spark)."""
    from pyspark.sql import functions as F

    if not cols:
        return F.lit(None).cast("bigint")
    return F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit(NULL_TOKEN)) for c in cols])


def key_expr(cols: tuple[str, ...] | list[str]):
    from pyspark.sql import functions as F

    return F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols])


def ensure_table(spark, spec: Scd2Spec, source_schema) -> None:
    by_name = {f.name: f for f in source_schema.fields}
    missing = [c for c in spec.data_cols if c not in by_name]
    if missing:
        raise KeyError(f"{spec.table}: source is missing columns {missing}")
    cols = [f"{c} {by_name[c].dataType.simpleString()}" for c in spec.data_cols]
    cols += [f"{c} {t}" for c, t in META_COLUMNS]
    comment = (spec.comment or f"SCD2 history of {spec.table}").replace("'", "")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {spec.table} ({', '.join(cols)}) USING DELTA
                  LOCATION '{spec.location}' COMMENT '{comment}'
                  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")


def prepare_source(df, spec: Scd2Spec):
    extra = [c for c in df.columns if c not in spec.data_cols]
    if extra:
        raise KeyError(f"{spec.table}: source has columns outside the spec: {extra}")
    return (df.select(*spec.data_cols)
            .withColumn("_key", key_expr(spec.key_cols))
            .withColumn("row_hash", hash_expr(spec.tracked_cols))
            .withColumn("t1_hash", hash_expr(spec.type1_cols)))


LAST_MONTH_PROP = "lambda.scd2.last_month"   # the table records the last month it applied (self-describing)


def last_month(spark, spec: Scd2Spec) -> int | None:
    rows = spark.sql(f"SHOW TBLPROPERTIES {spec.table}").collect()
    props = {r["key"]: r["value"] for r in rows}
    value = props.get(LAST_MONTH_PROP)
    return int(value) if value and value.isdigit() else None


def _set_last_month(spark, spec: Scd2Spec, month: int | None) -> None:
    spark.sql(f"ALTER TABLE {spec.table} SET TBLPROPERTIES ('{LAST_MONTH_PROP}' = '{month or ''}')")


def month_state(spark, spec: Scd2Spec, ref: RefMonth) -> dict[str, Any]:
    """What the table already holds relative to month M (used to decide apply / re-apply / refuse)."""
    from pyspark.sql import functions as F

    row = (spark.table(spec.table)
           .agg(F.max("valid_from").alias("max_from"), F.min("valid_from").alias("min_from"),
                F.sum(F.when(F.col("valid_from") == ref.yyyymm, 1).otherwise(0)).alias("born_in_m"),
                F.sum(F.when(F.col("valid_to") == ref.prev().yyyymm, 1).otherwise(0)).alias("closed_by_m"),
                F.count("*").alias("rows"))
           .first())
    state = {k: row[k] for k in ("max_from", "min_from", "born_in_m", "closed_by_m", "rows")}
    state["last_month"] = last_month(spark, spec)
    return state


def rollback(spark, spec: Scd2Spec, ref: RefMonth) -> dict[str, int]:
    """Undo month M, which must be the LAST month the table applied (its table property says so): a month without a
    single change leaves no trace in the rows, so the data alone cannot prove that M is the latest."""
    state = month_state(spark, spec, ref)
    applied = state["last_month"]
    if applied is not None and applied != ref.yyyymm:
        raise RuntimeError(f"{spec.table}: can only roll back its last applied month {applied}, not {ref}")
    spark.sql(f"DELETE FROM {spec.table} WHERE valid_from = {ref.yyyymm}")
    spark.sql(f"""UPDATE {spec.table} SET valid_to = NULL, is_current = true, closed_reason = NULL
                  WHERE valid_to = {ref.prev().yyyymm}""")
    _set_last_month(spark, spec, ref.prev().yyyymm if (state["min_from"] or 0) < ref.yyyymm else None)
    return {"deleted_versions": int(state["born_in_m"] or 0), "reopened_versions": int(state["closed_by_m"] or 0)}


def apply_month(spark, spec: Scd2Spec, source_df, ref: RefMonth, run_id: str) -> dict[str, int]:
    """Apply month M. `source_df` holds exactly one row per natural key with the spec's data columns."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    m, prev, run = ref.yyyymm, ref.prev().yyyymm, _lit(run_id)
    src = prepare_source(source_df, spec).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        current = spark.table(spec.table).where("is_current").select("_key", F.col("row_hash").alias("_cur_hash"))
        changed = (src.join(current, "_key", "inner").where(F.col("row_hash") != F.col("_cur_hash"))
                   .drop("_cur_hash"))
        changed_count = changed.count()
        source_count = src.count()
        staged = (src.withColumn("merge_key", F.col("_key")).withColumn("change_type", F.lit("NEW"))
                  .unionByName(changed.withColumn("merge_key", F.lit(None).cast("string"))
                               .withColumn("change_type", F.lit("CHANGED")))
                  .withColumn("_run_id", F.lit(run_id)))
        staged.createOrReplaceTempView("_scd2_staged")

        insert_cols = [*spec.data_cols, "_key", "row_hash", "t1_hash", "valid_from", "valid_to", "is_current",
                       "change_type", "closed_reason", "_run_id", "_updated_at"]
        insert_vals = [*(f"s.{c}" for c in spec.data_cols), "s._key", "s.row_hash", "s.t1_hash", str(m), "NULL",
                       "true", "s.change_type", "NULL", "s._run_id", "current_timestamp()"]
        type1_clause = ""
        if spec.type1_cols:
            sets = ", ".join(f"t.{c} = s.{c}" for c in spec.type1_cols)
            type1_clause = (f"WHEN MATCHED AND t.row_hash = s.row_hash AND NOT (t.t1_hash <=> s.t1_hash) THEN "
                            f"UPDATE SET {sets}, t.t1_hash = s.t1_hash, t._run_id = s._run_id, "
                            f"t._updated_at = current_timestamp()")
        spark.sql(f"""
            MERGE INTO {spec.table} AS t
            USING _scd2_staged AS s
            ON t._key = s.merge_key AND t.is_current = true
            WHEN MATCHED AND t.row_hash <> s.row_hash THEN
              UPDATE SET t.valid_to = {prev}, t.is_current = false, t.closed_reason = 'CHANGED',
                         t._run_id = s._run_id, t._updated_at = current_timestamp()
            {type1_clause}
            WHEN NOT MATCHED THEN
              INSERT ({', '.join(insert_cols)}) VALUES ({', '.join(insert_vals)})
            WHEN NOT MATCHED BY SOURCE AND t.is_current = true THEN
              UPDATE SET t.valid_to = {prev}, t.is_current = false, t.closed_reason = 'ABSENT',
                         t._run_id = {run}, t._updated_at = current_timestamp()
        """)
        metrics = merge_metrics(spark, spec.table)
        _set_last_month(spark, spec, m)
        inserted = metrics.get("numTargetRowsInserted", 0)
        absent = metrics.get("numTargetRowsNotMatchedBySourceUpdated", 0)
        matched_updated = metrics.get("numTargetRowsMatchedUpdated", 0)
        return {
            "source_rows": source_count, "new_keys": inserted - changed_count, "changed_keys": changed_count,
            "absent_keys": absent, "type1_updates": max(matched_updated - changed_count, 0),
            "unchanged_keys": source_count - (inserted - changed_count) - changed_count
                              - max(matched_updated - changed_count, 0),
            "rows_inserted": inserted, "rows_updated": matched_updated + absent,
        }
    finally:
        src.unpersist()


def merge_metrics(spark, table: str) -> dict[str, int]:
    row = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select("operation", "operationMetrics").first()
    if row["operation"] != "MERGE":
        raise RuntimeError(f"{table}: expected the last commit to be MERGE, found {row['operation']}")
    return {k: int(v) for k, v in (row["operationMetrics"] or {}).items() if str(v).lstrip("-").isdigit()}


def as_of(spark, table: str, ref: RefMonth):
    """The version of every key that was valid in month M (point-in-time view of an SCD2 table)."""
    m = ref.yyyymm
    return spark.table(table).where(f"valid_from <= {m} AND (valid_to IS NULL OR valid_to >= {m})")
