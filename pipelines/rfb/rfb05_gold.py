"""rfb05 - silver -> gold: consumption tables (Kimball style), built "as of" the reference month.

    python -m pipelines.rfb.rfb05_gold --ref-month 2026-09 --target dims
    python -m pipelines.rfb.rfb05_gold --ref-month 2026-09 --target estabelecimento_atual

Targets (database rfb_gold, s3a://gold/rfb/):
    dims                       dim_cnae, dim_municipio, dim_natureza_juridica, dim_qualificacao_socio, dim_pais,
                               dim_motivo_situacao (from silver code lists) + dim_codigo (fixed layout codes). SCD1.
    estabelecimento_atual      one row per CNPJ as of M: establishment + company + Simples/MEI + descriptions and
                               derived fields. Contacts (phones, e-mail) stay in silver: gold keeps PII to a minimum.
                               Partitioned by uf. Only rebuilt for the LATEST applied month (it is "atual").
    socio_atual                partners as of M with company name and descriptions.
    fato_mudanca_cadastral     event fact: one row per changed attribute (ALTERACAO), new key (INCLUSAO) and key that
                               disappeared (EXCLUSAO), per entity and month. Partitioned by ref_month.
    agg_estabelecimentos_mensal monthly counts by uf, municipality, CNAE division, status, size, Simples/MEI.

"As of M" uses the SCD2 validity window: valid_from <= M AND (valid_to IS NULL OR valid_to >= M), so the same code
builds the current month and any past month (backfill) correctly.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, datetime

from pipelines.commons.ctrl import DeltaCtrl
from pipelines.commons.delta_stats import record_table_stats
from pipelines.commons.logger import log_event
from pipelines.commons.refmonth import RefMonth
from pipelines.commons.scd2 import as_of
from pipelines.commons.settings import get_settings
from pipelines.commons.spark import spark_session
from pipelines.commons.task import TaskRun, TaskSkipped, run_task
from pipelines.rfb.contract import CODES
from pipelines.rfb.silver_model import DOMAIN_TABLES, MODELS, SILVER_DB

PIPELINE = "rfb"
GOLD_DB = "rfb_gold"
TARGETS = ("dims", "estabelecimento_atual", "socio_atual", "fato_mudanca_cadastral", "agg_estabelecimentos_mensal")
ORPHAN_RATE_MAX = 0.001
SPARK_CONF = {
    "spark.sql.parquet.compression.codec": "zstd",
    "spark.sql.shuffle.partitions": "96",
    "spark.sql.adaptive.enabled": "true",
}


def silver(name: str) -> str:
    return f"{SILVER_DB}.{name}"


def hist(entity: str) -> str:
    return silver(MODELS[entity].table)


def gold_location(table: str) -> str:
    s = get_settings()
    return s.lake_uri(s.bucket_gold, "rfb", table)


def write_table(spark, df, table: str, *, partition_by: list[str] | None = None, replace_where: str | None = None,
                comment: str = "") -> None:
    """Create on first write (LOCATION in the gold bucket), then overwrite (whole table or one partition)."""
    name = f"{GOLD_DB}.{table}"
    w = df.write.format("delta").mode("overwrite")
    if partition_by:
        w = w.partitionBy(*partition_by)
    if replace_where:
        w = w.option("replaceWhere", replace_where)
    else:
        w = w.option("overwriteSchema", "true")       # full rebuild may evolve the schema
    if not spark.catalog.tableExists(name):
        w = w.option("path", gold_location(table))
    w.saveAsTable(name)
    if comment:
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('comment' = '{comment.replace(chr(39), '')}')")


def _codes(spark):
    return spark.createDataFrame(list(CODES), "dominio string, codigo string, descricao string")


def code_desc(spark, dominio: str, alias: str):
    from pyspark.sql import functions as F

    return F.broadcast(_codes(spark).where(F.col("dominio") == dominio)
                       .select(F.col("codigo").alias(f"_{alias}_cod"), F.col("descricao").alias(alias)))


def dim(spark, table: str, alias: str):
    from pyspark.sql import functions as F

    return F.broadcast(spark.table(silver(table))
                       .select(F.col("codigo").alias(f"_{alias}_cod"), F.col("descricao").alias(alias)))


# ----------------------------------------------------------------------------------------------- targets
def build_dims(task: TaskRun, spark, ref: RefMonth) -> int:
    total = 0
    for table in DOMAIN_TABLES.values():
        df = spark.table(silver(table)).select("codigo", "descricao")
        write_table(spark, df, f"dim_{table}", comment=f"RFB code list {table} (SCD1, current)")
        total += record_table_stats(task, spark, f"{GOLD_DB}.dim_{table}", "gold", ref_month=ref.yyyymm) \
            .get("rows_total") or 0
    write_table(spark, _codes(spark), "dim_codigo", comment="Fixed RFB layout codes (situacao, porte, socio...)")
    record_table_stats(task, spark, f"{GOLD_DB}.dim_codigo", "gold", ref_month=ref.yyyymm)
    return total + len(CODES)


def estabelecimento_asof(spark, ref: RefMonth):
    from pyspark.sql import functions as F

    m_end = F.last_day(F.to_date(F.lit(f"{ref.iso}-01")))
    e = as_of(spark, hist("estabelecimentos"), ref).alias("e")
    emp = as_of(spark, hist("empresas"), ref).select(
        "cnpj_basico", "razao_social", "natureza_juridica", "porte_empresa", "capital_social",
        "qualificacao_responsavel", "ente_federativo_responsavel")
    sim = as_of(spark, hist("simples"), ref).select("cnpj_basico", "opcao_simples", "opcao_mei",
                                                    "data_opcao_simples", "data_opcao_mei")
    df = (e.join(emp, "cnpj_basico", "left").join(sim, "cnpj_basico", "left")
          .join(dim(spark, "cnae", "cnae_principal_desc"),
                F.col("cnae_fiscal_principal") == F.col("_cnae_principal_desc_cod"), "left")
          .join(dim(spark, "municipio", "municipio_nome"), F.col("municipio") == F.col("_municipio_nome_cod"), "left")
          .join(dim(spark, "natureza_juridica", "natureza_juridica_desc"),
                F.col("natureza_juridica") == F.col("_natureza_juridica_desc_cod"), "left")
          .join(dim(spark, "motivo_situacao", "motivo_situacao_desc"),
                F.col("motivo_situacao_cadastral") == F.col("_motivo_situacao_desc_cod"), "left")
          .join(dim(spark, "pais", "pais_desc"), F.col("pais") == F.col("_pais_desc_cod"), "left")
          .join(code_desc(spark, "situacao_cadastral", "situacao_cadastral_desc"),
                F.col("situacao_cadastral") == F.col("_situacao_cadastral_desc_cod"), "left")
          .join(code_desc(spark, "porte_empresa", "porte_empresa_desc"),
                F.col("porte_empresa") == F.col("_porte_empresa_desc_cod"), "left")
          .join(code_desc(spark, "identificador_matriz_filial", "matriz_filial_desc"),
                F.col("identificador_matriz_filial") == F.col("_matriz_filial_desc_cod"), "left"))
    return df.select(
        "cnpj", "cnpj_basico", "cnpj_ordem", "cnpj_dv", "cnpj_dv_valido",
        (F.col("identificador_matriz_filial") == "1").alias("is_matriz"), "matriz_filial_desc",
        "razao_social", "nome_fantasia",
        "situacao_cadastral", "situacao_cadastral_desc", (F.col("situacao_cadastral") == "02").alias("is_ativo"),
        "data_situacao_cadastral", "motivo_situacao_cadastral", "motivo_situacao_desc",
        "data_inicio_atividade",
        F.round(F.months_between(m_end, F.col("data_inicio_atividade")) / 12, 1).alias("idade_anos"),
        "cnae_fiscal_principal", "cnae_principal_desc",
        F.substring("cnae_fiscal_principal", 1, 2).alias("cnae_divisao"),
        "cnae_fiscal_secundaria", F.coalesce(F.size("cnae_fiscal_secundaria"), F.lit(0)).alias("qtd_cnae_secundaria"),
        "natureza_juridica", "natureza_juridica_desc", "qualificacao_responsavel", "ente_federativo_responsavel",
        "porte_empresa", "porte_empresa_desc", "capital_social",
        F.coalesce("opcao_simples", F.lit(False)).alias("opcao_simples"), "data_opcao_simples",
        F.coalesce("opcao_mei", F.lit(False)).alias("opcao_mei"), "data_opcao_mei",
        "tipo_logradouro", "logradouro", "numero", "complemento", "bairro", "cep",
        "municipio", "municipio_nome", "uf", "pais", "pais_desc", "nome_cidade_exterior",
        "situacao_especial", "data_situacao_especial",
        F.lit(ref.yyyymm).alias("_ref_month"), F.lit(datetime.now(UTC)).cast("timestamp").alias("_built_at"),
    )


def build_estabelecimento_atual(task: TaskRun, spark, ctrl: DeltaCtrl, ref: RefMonth) -> int:
    from pyspark.sql import functions as F

    latest = ctrl.last_applied(PIPELINE, "silver.estabelecimentos")
    if latest and ref.yyyymm < latest:
        raise TaskSkipped(f"estabelecimento_atual reflects {latest}; {ref} is a past month (backfill)")
    df = estabelecimento_asof(spark, ref).withColumn("_run_id", F.lit(task.ctx.run_id))
    write_table(spark, df, "estabelecimento_atual", partition_by=["uf"],
                comment="One row per CNPJ as of the latest month: establishment + company + Simples/MEI + descriptions")
    stats = record_table_stats(task, spark, f"{GOLD_DB}.estabelecimento_atual", "gold", ref_month=ref.yyyymm)
    expected = as_of(spark, hist("estabelecimentos"), ref).count()
    rows = stats.get("rows_total") or spark.table(f"{GOLD_DB}.estabelecimento_atual").count()
    task.check("gold.rows_nonzero", rows > 0, observed=rows, threshold=1, message="estabelecimento_atual rows")
    task.check("gold.rows_match_silver", rows == expected, observed=rows, threshold=expected,
               message="estabelecimento_atual vs silver establishments as of the month (join fan-out guard)")
    g = spark.table(f"{GOLD_DB}.estabelecimento_atual")
    orphans = g.agg(F.sum(F.when(F.col("cnae_principal_desc").isNull() & F.col("cnae_fiscal_principal").isNotNull(), 1)
                          .otherwise(0)).alias("cnae"),
                    F.sum(F.when(F.col("municipio_nome").isNull() & F.col("municipio").isNotNull(), 1)
                          .otherwise(0)).alias("municipio")).first().asDict()
    worst = max(int(v or 0) for v in orphans.values())
    task.check("gold.orphan_rate", worst <= ORPHAN_RATE_MAX * max(rows, 1), observed=worst,
               threshold=round(ORPHAN_RATE_MAX * rows), per_dimension={k: int(v or 0) for k, v in orphans.items()},
               message="codes without a description in the dimension")
    return rows


def build_socio_atual(task: TaskRun, spark, ctrl: DeltaCtrl, ref: RefMonth) -> int:
    from pyspark.sql import functions as F

    latest = ctrl.last_applied(PIPELINE, "silver.socios")
    if latest and ref.yyyymm < latest:
        raise TaskSkipped(f"socio_atual reflects {latest}; {ref} is a past month (backfill)")
    s = as_of(spark, hist("socios"), ref)
    emp = as_of(spark, hist("empresas"), ref).select("cnpj_basico", "razao_social")
    df = (s.join(emp, "cnpj_basico", "left")
          .join(dim(spark, "qualificacao_socio", "qualificacao_socio_desc"),
                F.col("qualificacao_socio") == F.col("_qualificacao_socio_desc_cod"), "left")
          .join(code_desc(spark, "identificador_socio", "identificador_socio_desc"),
                F.col("identificador_socio") == F.col("_identificador_socio_desc_cod"), "left")
          .join(code_desc(spark, "faixa_etaria", "faixa_etaria_desc"),
                F.col("faixa_etaria") == F.col("_faixa_etaria_desc_cod"), "left")
          .join(dim(spark, "pais", "pais_desc"), F.col("pais") == F.col("_pais_desc_cod"), "left")
          .select("cnpj_basico", "razao_social", "identificador_socio", "identificador_socio_desc", "nome_socio",
                  "cnpj_cpf_socio", "qualificacao_socio", "qualificacao_socio_desc", "data_entrada_sociedade",
                  "pais", "pais_desc", "representante_legal", "nome_representante", "qualificacao_representante",
                  "faixa_etaria", "faixa_etaria_desc",
                  F.lit(ref.yyyymm).alias("_ref_month"), F.lit(task.ctx.run_id).alias("_run_id")))
    write_table(spark, df, "socio_atual", comment="Partners as of the latest month, with company name and descriptions")
    stats = record_table_stats(task, spark, f"{GOLD_DB}.socio_atual", "gold", ref_month=ref.yyyymm)
    expected = s.count()
    rows = stats.get("rows_total") or spark.table(f"{GOLD_DB}.socio_atual").count()
    task.check("gold.rows_nonzero", rows > 0, observed=rows, threshold=1, message="socio_atual rows")
    task.check("gold.rows_match_silver", rows == expected, observed=rows, threshold=expected,
               message="socio_atual vs silver partners as of the month (join fan-out guard)")
    return rows


def change_events(spark, entity: str, ref: RefMonth):
    """INCLUSAO / ALTERACAO (one row per changed attribute) / EXCLUSAO events of one SCD2 table for month M."""
    from pyspark.sql import functions as F

    model = MODELS[entity]
    h = spark.table(hist(entity))
    m, p = ref.yyyymm, ref.prev().yyyymm
    baseline = h.agg(F.min("valid_from")).first()[0] == m        # first month loaded: everything is "new"
    new_v = h.where((F.col("valid_from") == m) & (F.col("change_type") == "CHANGED")).alias("n")
    old_v = h.where((F.col("valid_to") == p) & (F.col("closed_reason") == "CHANGED")).alias("o")
    diffs = F.filter(F.array(*[
        F.when(~F.col(f"o.{c}").eqNullSafe(F.col(f"n.{c}")),
               F.struct(F.lit(c).alias("atributo"), F.col(f"o.{c}").cast("string").alias("valor_anterior"),
                        F.col(f"n.{c}").cast("string").alias("valor_novo")))
        for c in model.tracked_cols]), lambda x: x.isNotNull())
    alteracoes = (new_v.join(old_v, F.col("n._key") == F.col("o._key"))
                  .select(F.col("n._key").alias("chave"), F.col("n.cnpj_basico").alias("cnpj_basico"),
                          F.explode(diffs).alias("d"))
                  .select("chave", "cnpj_basico", F.lit("ALTERACAO").alias("tipo_evento"), "d.atributo",
                          "d.valor_anterior", "d.valor_novo"))
    blank = [F.lit(None).cast("string").alias(c) for c in ("atributo", "valor_anterior", "valor_novo")]
    exclusoes = (h.where((F.col("valid_to") == p) & (F.col("closed_reason") == "ABSENT"))
                 .select(F.col("_key").alias("chave"), "cnpj_basico", F.lit("EXCLUSAO").alias("tipo_evento"), *blank))
    events = alteracoes.unionByName(exclusoes)
    if not baseline:
        inclusoes = (h.where((F.col("valid_from") == m) & (F.col("change_type") == "NEW"))
                     .select(F.col("_key").alias("chave"), "cnpj_basico", F.lit("INCLUSAO").alias("tipo_evento"),
                             *blank))
        events = events.unionByName(inclusoes)
    return events.withColumn("entidade", F.lit(entity)), baseline


def build_fato_mudanca(task: TaskRun, spark, ref: RefMonth) -> int:
    from pyspark.sql import functions as F

    frames, baselines = [], {}
    for entity in MODELS:
        ev, baselines[entity] = change_events(spark, entity, ref)
        frames.append(ev)
    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f)
    df = df.select(F.lit(ref.yyyymm).alias("ref_month"), "entidade", "chave", "cnpj_basico", "tipo_evento",
                   "atributo", "valor_anterior", "valor_novo", F.lit(task.ctx.run_id).alias("_run_id"))
    write_table(spark, df, "fato_mudanca_cadastral", partition_by=["ref_month"],
                replace_where=f"ref_month = {ref.yyyymm}",
                comment="Change log from the SCD2 history: INCLUSAO, ALTERACAO (per attribute), EXCLUSAO by month")
    record_table_stats(task, spark, f"{GOLD_DB}.fato_mudanca_cadastral", "gold", ref_month=ref.yyyymm)
    by_type = {f"{r['entidade']}.{r['tipo_evento']}": r["n"] for r in
               spark.table(f"{GOLD_DB}.fato_mudanca_cadastral").where(F.col("ref_month") == ref.yyyymm)
               .groupBy("entidade", "tipo_evento").agg(F.count("*").alias("n")).collect()}
    log_event(task.log, "change_events", f"{ref}: {sum(by_type.values()):,} change events "
              f"(baseline month for: {[e for e, b in baselines.items() if b] or 'none'})",
              events=by_type, baseline=baselines)
    task.metric(change_events=by_type)
    return sum(by_type.values())


def build_agg_mensal(task: TaskRun, spark, ref: RefMonth) -> int:
    from pyspark.sql import functions as F

    m_start = F.to_date(F.lit(f"{ref.iso}-01"))
    e = as_of(spark, hist("estabelecimentos"), ref)
    emp = as_of(spark, hist("empresas"), ref).select("cnpj_basico", "porte_empresa", "natureza_juridica",
                                                     "capital_social")
    sim = as_of(spark, hist("simples"), ref).select("cnpj_basico", "opcao_simples", "opcao_mei")
    base = e.join(emp, "cnpj_basico", "left").join(sim, "cnpj_basico", "left")
    in_month = lambda c: (F.col(c) >= m_start) & (F.col(c) <= F.last_day(m_start))  # noqa: E731
    df = (base.groupBy("uf", "municipio", F.substring("cnae_fiscal_principal", 1, 2).alias("cnae_divisao"),
                       "situacao_cadastral", "porte_empresa", "natureza_juridica",
                       F.coalesce("opcao_simples", F.lit(False)).alias("opcao_simples"),
                       F.coalesce("opcao_mei", F.lit(False)).alias("opcao_mei"),
                       (F.col("identificador_matriz_filial") == "1").alias("is_matriz"))
          .agg(F.count("*").alias("qtd_estabelecimentos"),
               F.sum(F.when(in_month("data_inicio_atividade"), 1).otherwise(0)).alias("qtd_abertos_no_mes"),
               F.sum(F.when((F.col("situacao_cadastral") == "08") & in_month("data_situacao_cadastral"), 1)
                     .otherwise(0)).alias("qtd_baixados_no_mes"),
               F.sum(F.when(F.col("identificador_matriz_filial") == "1", F.col("capital_social")))
               .alias("capital_social_matrizes"))
          .withColumn("_ref_month", F.lit(ref.yyyymm)).withColumn("_run_id", F.lit(task.ctx.run_id)))
    write_table(spark, df, "agg_estabelecimentos_mensal", partition_by=["_ref_month"],
                replace_where=f"_ref_month = {ref.yyyymm}",
                comment="Monthly establishment counts by uf, municipality, CNAE division, status, size, Simples/MEI")
    stats = record_table_stats(task, spark, f"{GOLD_DB}.agg_estabelecimentos_mensal", "gold", ref_month=ref.yyyymm)
    total = spark.table(f"{GOLD_DB}.agg_estabelecimentos_mensal").where(F.col("_ref_month") == ref.yyyymm) \
        .agg(F.sum("qtd_estabelecimentos")).first()[0] or 0
    expected = e.count()
    task.check("gold.rows_match_silver", total == expected, observed=total, threshold=expected,
               message="sum of qtd_estabelecimentos vs establishments as of the month (no loss in aggregation)")
    return stats.get("rows_written") or 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--ref-month", required=True, help="YYYY-MM or YYYYMM")
    parser.add_argument("--target", required=True, choices=TARGETS)
    parser.add_argument("--master", default=None, help="Spark master (default: the cluster in spark-defaults)")
    args = parser.parse_args(argv)
    try:
        ref = RefMonth.parse(args.ref_month)
    except ValueError as exc:
        parser.error(str(exc))

    def body(task: TaskRun) -> None:
        s = get_settings()
        t0 = time.monotonic()
        with spark_session(f"rfb05_gold-{args.target}", master=args.master, conf=SPARK_CONF) as spark:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB} COMMENT 'RFB consumption layer (Kimball)' "
                      f"LOCATION '{s.lake_uri(s.bucket_gold, 'rfb')}/'")
            ctrl = DeltaCtrl(spark, bucket=s.bucket_ctrl).ensure()
            layer = f"gold.{args.target}"
            builders = {
                "dims": lambda: build_dims(task, spark, ref),
                "estabelecimento_atual": lambda: build_estabelecimento_atual(task, spark, ctrl, ref),
                "socio_atual": lambda: build_socio_atual(task, spark, ctrl, ref),
                "fato_mudanca_cadastral": lambda: build_fato_mudanca(task, spark, ref),
                "agg_estabelecimentos_mensal": lambda: build_agg_mensal(task, spark, ref),
            }
            rows = builders[args.target]()
            ctrl.set_watermark({"pipeline": PIPELINE, "layer": layer, "ref_month": ref.yyyymm, "status": "DONE",
                                "rows": rows, "run_id": task.ctx.run_id, "applied_at": datetime.now(UTC),
                                "updated_at": datetime.now(UTC)})
            task.add(rows_out=rows)
            log_event(task.log, "gold_done", f"{args.target}: {rows:,} rows in {time.monotonic() - t0:.0f}s",
                      target=args.target, rows=rows, seconds=round(time.monotonic() - t0, 1))

    return run_task(body, pipeline=PIPELINE, task_id=f"rfb05_gold.{args.target}", layer="gold", ref_month=ref,
                    entity=args.target, params={"target": args.target})


if __name__ == "__main__":
    sys.exit(main())
