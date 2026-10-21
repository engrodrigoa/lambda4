"""rfb04 - bronze month -> silver: typed, cleaned, deduplicated, and applied as SCD2 history (Delta MERGE).

    python -m pipelines.rfb.rfb04_silver --ref-month 2026-09 --entity estabelecimentos
    python -m pipelines.rfb.rfb04_silver --ref-month 2026-09 --entity domains        # SCD1 snapshot tables

Tables (database rfb_silver, s3a://silver/rfb/):
    estabelecimento_hist  key cnpj (14)                       contacts are type 1 (overwritten, no new version)
    empresa_hist          key cnpj_basico
    simples_hist          key cnpj_basico
    socio_hist            key cnpj_basico + identificador_socio + cnpj_cpf_socio + nome_socio
    cnae, motivo_situacao, municipio, natureza_juridica, pais, qualificacao_socio   (SCD1: current list)

Month ordering (BLOCK silver.month_order), from ctrl.layer_watermark `silver.<entity>`:
    first load (no watermark)        -> apply M (every key is NEW: the baseline)
    M == last applied + 1            -> apply M
    M == last applied                -> re-run: rollback(M) then apply M (idempotent)
    the table already holds M but the watermark does not (crash after the MERGE) -> rollback(M) then apply
    anything else (gap or past month) -> BLOCK; use --allow-gap only for a month the Receita never published
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, datetime

from pipelines.commons import scd2
from pipelines.commons.ctrl import DeltaCtrl
from pipelines.commons.delta_stats import record_table_stats
from pipelines.commons.logger import log_event
from pipelines.commons.quarantine import Quarantine
from pipelines.commons.refmonth import RefMonth
from pipelines.commons.settings import get_settings
from pipelines.commons.spark import spark_session
from pipelines.commons.task import TaskRun, run_task
from pipelines.rfb.contract import expand_entities
from pipelines.rfb.silver_model import (
    DOMAIN_TABLES,
    MODELS,
    SILVER_DB,
    bronze_partition,
    dedup,
    invalid_date,
    transform,
    transform_domain,
)

PIPELINE = "rfb"
CHANGE_RATE_MAX = 0.20          # silver.change_rate: >20 % of keys changing in one month is an anomaly signal
INVALID_DATE_RATE_MAX = 0.001
DV_INVALID_RATE_MAX = 0.001
SPARK_CONF = {
    "spark.sql.parquet.compression.codec": "zstd",
    "spark.sql.shuffle.partitions": "96",         # AQE coalesces small partitions at runtime
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
}


def _now() -> datetime:
    return datetime.now(UTC)


def _watermark(ctrl, layer: str, ref: RefMonth, status: str, task: TaskRun, rows: int | None = None) -> None:
    ctrl.set_watermark({"pipeline": PIPELINE, "layer": layer, "ref_month": ref.yyyymm, "status": status,
                        "rows": rows, "run_id": task.ctx.run_id,
                        "applied_at": _now() if status == "DONE" else None, "updated_at": _now()})


def decide_order(last_applied: int | None, ref: RefMonth, allow_gap: bool) -> str:
    """'apply' | 'reapply' | 'block:<reason>' (pure function: unit-tested)."""
    if last_applied is None:
        return "apply"
    last = RefMonth.parse(last_applied)
    if ref == last:
        return "reapply"
    if ref == last.next():
        return "apply"
    if ref > last.next():
        return "apply" if allow_gap else f"block:gap ({last} applied, {ref} requested, missing {last.next()})"
    return f"block:past month ({ref} requested, {last} already applied; rebuild to change history)"


def load_entity(task: TaskRun, spark, ctrl: DeltaCtrl, quarantine: Quarantine, ref: RefMonth, entity: str,
                allow_gap: bool) -> dict:
    from pyspark.sql import functions as F

    log = task.log
    model = MODELS[entity]
    spec = model.spec()
    layer = f"silver.{entity}"
    t0 = time.monotonic()

    # ---------------------------------------------------------------- preconditions (BLOCK)
    bronze_wm = ctrl.get_watermark(PIPELINE, f"bronze.{entity}", ref.yyyymm) or {}
    task.check("silver.bronze_month_present", bronze_wm.get("status") == "DONE", entity=entity,
               message=f"{entity}: bronze watermark {ref} = {bronze_wm.get('status', 'absent')}")
    bronze_rows = int(bronze_wm.get("rows") or 0)
    decision = decide_order(ctrl.last_applied(PIPELINE, layer), ref, allow_gap)
    task.check("silver.month_order", not decision.startswith("block"), entity=entity,
               message=f"{entity}: {decision}")

    # ---------------------------------------------------------------- bronze -> typed -> dedup
    bronze = bronze_partition(spark, entity, ref)
    typed = transform(entity, bronze)
    if model.date_cols:
        bad = bronze.agg(*[F.sum(invalid_date(c)).alias(c) for c in model.date_cols]).first().asDict()
        total_bad = sum(int(v or 0) for v in bad.values())
        task.check("silver.invalid_dates", total_bad <= INVALID_DATE_RATE_MAX * max(bronze_rows, 1),
                   observed=total_bad, threshold=round(INVALID_DATE_RATE_MAX * bronze_rows), entity=entity,
                   per_column={k: int(v or 0) for k, v in bad.items()},
                   message=f"{entity}: unparseable non-zero dates (set to NULL)")

    winners, losers, ranked = dedup(typed, model)
    ranked = ranked.persist()
    counts = ranked.agg(F.count("*").alias("distinct_rows"),
                        F.sum(F.when(F.col("_rn") > 1, 1).otherwise(0)).alias("conflicts")).first()
    distinct_rows, conflicts = int(counts["distinct_rows"]), int(counts["conflicts"] or 0)
    exact_dups = bronze_rows - distinct_rows
    task.check("silver.duplicates_removed", exact_dups == 0, observed=exact_dups, threshold=0, entity=entity,
               message=f"{entity}: exact duplicate rows dropped")
    task.check("silver.conflicting_duplicates", conflicts == 0, observed=conflicts, threshold=0, entity=entity,
               message=f"{entity}: same key with different values (winner kept, others quarantined)")
    # always replace: a re-run with zero conflicts must also clear the rows of the previous run
    q = None
    if conflicts:
        q = losers.select(F.lit(f"{entity}").alias("source_file"), F.lit("silver").alias("stage"),
                          F.lit("conflicting_duplicate").alias("reason"), F.lit(None).cast("bigint").alias("line_no"),
                          F.to_json(F.struct(*typed.columns)).alias("raw_record"))
    quarantined = quarantine.replace(pipeline=PIPELINE, ref_month=ref.yyyymm, entity=f"silver.{entity}", df=q,
                                     rows=None, run_id=task.ctx.run_id, task_id=task.task_id)
    source = winners
    if entity == "estabelecimentos":
        dv_bad = source.where(~F.col("cnpj_dv_valido")).count()
        task.check("silver.cnpj_dv_valid", dv_bad <= DV_INVALID_RATE_MAX * max(bronze_rows, 1), observed=dv_bad,
                   threshold=round(DV_INVALID_RATE_MAX * bronze_rows), entity=entity,
                   message=f"{entity}: CNPJs whose check digits do not verify")
    source_rows = distinct_rows - conflicts
    keys = source.select(*model.key_cols).distinct().count()
    task.check("silver.natural_key_unique", keys == source_rows, observed=source_rows - keys, threshold=0,
               entity=entity, message=f"{entity}: rows {source_rows:,} vs distinct keys {keys:,} after dedup")

    # ---------------------------------------------------------------- SCD2
    scd2.ensure_table(spark, spec, source.schema)
    state = scd2.month_state(spark, spec, ref)
    if decision == "reapply" or (state["born_in_m"] or 0) > 0:
        rb = scd2.rollback(spark, spec, ref)
        log_event(log, "scd2_rollback", f"{entity}: re-applying {ref}: removed {rb['deleted_versions']:,} versions, "
                  f"re-opened {rb['reopened_versions']:,}", entity=entity, **rb)
    first_load = state["min_from"] is None or state["min_from"] == ref.yyyymm
    _watermark(ctrl, layer, ref, "RUNNING", task)
    t_merge = time.monotonic()
    try:
        m = scd2.apply_month(spark, spec, source, ref, task.ctx.run_id)
    except Exception:
        _watermark(ctrl, layer, ref, "FAILED", task)
        raise
    finally:
        ranked.unpersist()
    merge_s = time.monotonic() - t_merge
    log_event(log, "scd2_merged", f"{entity}: {m['new_keys']:,} new, {m['changed_keys']:,} changed, "
              f"{m['absent_keys']:,} absent, {m['type1_updates']:,} type-1, {m['unchanged_keys']:,} unchanged "
              f"in {merge_s:.0f}s", entity=entity, seconds=round(merge_s, 1), first_load=first_load, **m)

    current = spark.table(spec.table).where("is_current").count()
    task.check("silver.current_rows_match_source", current == source_rows, observed=current, threshold=source_rows,
               entity=entity, message=f"{entity}: current versions vs source rows after dedup")
    if not first_load:
        rate = m["changed_keys"] / max(source_rows, 1)
        task.check("silver.change_rate", rate <= CHANGE_RATE_MAX, observed=round(rate, 5),
                   threshold=CHANGE_RATE_MAX, entity=entity, message=f"{entity}: share of keys that changed")

    _watermark(ctrl, layer, ref, "DONE", task, rows=current)
    stats = record_table_stats(task, spark, spec.table, "silver", ref_month=ref.yyyymm)
    task.add(rows_in=bronze_rows, rows_out=m["rows_inserted"] + m["rows_updated"], rows_rejected=quarantined)
    elapsed = time.monotonic() - t0
    log_event(log, "silver_done", f"{entity}: {current:,} current keys, {stats.get('rows_total', 0) or 0:,} versions "
              f"in history, {elapsed:.0f}s", entity=entity, seconds=round(elapsed, 1), current_rows=current,
              history_rows=stats.get("rows_total"), exact_duplicates=exact_dups, conflicts=conflicts)
    return {**m, "current_rows": current, "exact_duplicates": exact_dups, "conflicts": conflicts,
            "seconds": round(elapsed, 1)}


def load_domains(task: TaskRun, spark, ctrl: DeltaCtrl, ref: RefMonth) -> dict:
    """Small code lists: SCD1 snapshot (the list as published in month M), overwritten each run."""
    s = get_settings()
    out = {}
    for entity, table in DOMAIN_TABLES.items():
        layer = f"silver.{entity}"
        wm = ctrl.get_watermark(PIPELINE, f"bronze.{entity}", ref.yyyymm) or {}
        task.check("silver.bronze_month_present", wm.get("status") == "DONE", entity=entity,
                   message=f"{entity}: bronze watermark {ref} = {wm.get('status', 'absent')}")
        df = transform_domain(bronze_partition(spark, entity, ref))
        rows = df.count()
        keys = df.select("codigo").distinct().count()
        task.check("silver.natural_key_unique", rows == keys, observed=rows - keys, threshold=0, entity=entity,
                   message=f"{entity}: codes must be unique")
        name = f"{SILVER_DB}.{table}"
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {name} (codigo string, descricao string, _ref_month int)
                      USING DELTA LOCATION '{s.lake_uri(s.bucket_silver, 'rfb', table)}'
                      COMMENT 'RFB domain {entity}: current list (SCD1)'""")
        df.write.format("delta").mode("overwrite").saveAsTable(name)
        _watermark(ctrl, layer, ref, "DONE", task, rows=rows)
        record_table_stats(task, spark, name, "silver", ref_month=ref.yyyymm)
        task.add(rows_in=rows, rows_out=rows)
        out[entity] = rows
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--ref-month", required=True, help="YYYY-MM or YYYYMM")
    parser.add_argument("--entity", required=True, help="estabelecimentos|empresas|socios|simples|domains")
    parser.add_argument("--allow-gap", action="store_true", help="accept a month gap (source skipped a month)")
    parser.add_argument("--master", default=None, help="Spark master (default: the cluster in spark-defaults)")
    args = parser.parse_args(argv)
    try:
        ref = RefMonth.parse(args.ref_month)
        if args.entity not in (*MODELS, "domains"):
            raise KeyError(f"silver entity must be one of {', '.join((*MODELS, 'domains'))}")
        entities = expand_entities(args.entity)
    except (ValueError, KeyError) as exc:
        parser.error(str(exc))

    def body(task: TaskRun) -> None:
        s = get_settings()
        with spark_session(f"rfb04_silver-{args.entity}", master=args.master, conf=SPARK_CONF) as spark:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB} COMMENT 'RFB typed, deduplicated, SCD2 history' "
                      f"LOCATION '{s.lake_uri(s.bucket_silver, 'rfb')}/'")
            ctrl = DeltaCtrl(spark, bucket=s.bucket_ctrl).ensure()
            if args.entity == "domains":
                task.metric(domains=load_domains(task, spark, ctrl, ref))
                return
            quarantine = Quarantine(spark, bucket=s.bucket_audit).ensure()
            task.metric(entities={e: load_entity(task, spark, ctrl, quarantine, ref, e, args.allow_gap)
                                  for e in entities})

    return run_task(body, pipeline=PIPELINE, task_id=f"rfb04_silver.{args.entity}", layer="silver", ref_month=ref,
                    entity=args.entity, params={"entity": args.entity, "allow_gap": args.allow_gap})


if __name__ == "__main__":
    sys.exit(main())
