"""Silver model for the RFB entities: typing, cleaning, natural keys, SCD2 specs and dedup policy.

Typing rules (decided from the 2026-09 profile, see docs/learning/04):
* Codes stay STRINGS even when they look numeric: CNPJ, CEP, CNAE, municipality, legal nature, qualifications.
  They carry leading zeros and, since July 2026, the CNPJ can be ALPHANUMERIC. Arithmetic on a code is a bug.
* Dates 'yyyyMMdd'; '', '0' and '00000000' mean "no date" -> NULL (not an error). Anything else unparseable is
  counted (silver.invalid_dates) and becomes NULL.
* Money 'nnnn,dd' (Brazilian decimal comma) -> decimal(18,2). Flags 'S'/'N' -> boolean.
* Text: SUB (\\x1a, a character lost at the source) -> '?', other control chars removed, newlines -> space,
  whitespace collapsed and trimmed, '' -> NULL.
"""
from __future__ import annotations

from dataclasses import dataclass

from pipelines.commons.refmonth import RefMonth
from pipelines.commons.scd2 import Scd2Spec
from pipelines.commons.settings import get_settings

SILVER_DB = "rfb_silver"
ZERO_DATES = ("", "0", "00000000")
# control chars removed from text: C0 except TAB, LF, CR (LF/CR become spaces) and SUB (\x1a, becomes '?')
_CONTROL_CHARS = "".join(chr(c) for c in (*range(0x00, 0x09), 0x0B, 0x0C, *range(0x0E, 0x20)) if c != 0x1A)


# ----------------------------------------------------------------------------------------------- column helpers
def clean_text(c):
    """Performance-critical (10 text columns x ~70 M rows): `translate` maps/deletes single chars without a regex,
    and the one regex (collapse whitespace runs) only runs on values that can contain a run. Four regexes per value
    doubled the CPU of the whole typing (estabelecimentos: 73 -> 34 us/row, identical output; ADR 0005)."""
    from pyspark.sql import functions as F

    # SUB -> '?', CR/LF -> ' ', other control chars deleted (translate drops chars beyond the replacement string)
    v = F.translate(F.col(c) if isinstance(c, str) else c, "\u001a\r\n" + _CONTROL_CHARS, "?  ")
    # a run of 2+ whitespace needs either two spaces or a TAB (the only whitespace left): skip the regex otherwise
    v = F.trim(F.when(v.contains("  ") | v.contains("\t"), F.regexp_replace(v, r"\s{2,}", " ")).otherwise(v))
    return F.when(v == "", None).otherwise(v)


def code(c, width: int | None = None):
    """Trimmed code string; numeric codes are left-padded to `width` (e.g. CEP lost its leading zero)."""
    from pyspark.sql import functions as F

    v = F.trim(F.col(c))
    if width:
        # the regex only runs when the length is off (almost never): a value already `width` long is kept as is
        v = F.when((F.length(v) != width) & v.rlike(r"^[0-9]+$"), F.lpad(v, width, "0")).otherwise(v)
    return F.when((v == "") | v.isNull(), None).otherwise(v)


def rfb_date(c):
    from pyspark.sql import functions as F

    v = F.trim(F.col(c))
    return F.when(v.isNull() | v.isin(*ZERO_DATES), None).otherwise(
        F.try_to_timestamp(v, F.lit("yyyyMMdd")).cast("date"))


def invalid_date(c):
    """1 when the source had a non-empty, non-zero value that is not a valid date."""
    from pyspark.sql import functions as F

    v = F.trim(F.col(c))
    return F.when(v.isNotNull() & ~v.isin(*ZERO_DATES) & F.try_to_timestamp(v, F.lit("yyyyMMdd")).isNull(), 1) \
        .otherwise(0)


def flag_sn(c):
    from pyspark.sql import functions as F

    v = F.upper(F.trim(F.col(c)))
    return F.when(v == "S", True).when(v == "N", False).otherwise(None)


def money_br(c):
    from pyspark.sql import functions as F

    return F.regexp_replace(F.trim(F.col(c)), ",", ".").try_cast("decimal(18,2)")


def cnpj_dv_ok(basico: str, ordem: str, dv: str):
    """Official check-digit rule; char value = ascii(c) - 48 works for digits AND the new alphanumeric CNPJ."""
    from pyspark.sql import functions as F

    base = F.concat(F.col(basico), F.col(ordem))                      # 12 chars

    def digit(expr):
        r = expr % 11
        return F.when(r < 2, 0).otherwise(11 - r)

    w1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    w2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    vals = [F.ascii(F.substring(base, i + 1, 1)) - 48 for i in range(12)]
    d1 = digit(sum(v * w for v, w in zip(vals, w1, strict=True)))
    d2 = digit(sum(v * w for v, w in zip([*vals, d1], w2, strict=True)))
    return (F.length(base) == 12) & (F.concat(d1.cast("string"), d2.cast("string")) == F.col(dv))


# ----------------------------------------------------------------------------------------------- entity model
@dataclass(frozen=True)
class EntityModel:
    entity: str                      # bronze entity
    table: str                       # silver table name (without database)
    key_cols: tuple[str, ...]
    tracked_cols: tuple[str, ...]
    type1_cols: tuple[str, ...]
    dedup_order: tuple[str, ...]     # winner among conflicting duplicates: first row of this ordering
    date_cols: tuple[str, ...]       # bronze columns checked by silver.invalid_dates

    def spec(self) -> Scd2Spec:
        s = get_settings()
        return Scd2Spec(table=f"{SILVER_DB}.{self.table}", location=s.lake_uri(s.bucket_silver, "rfb", self.table),
                        key_cols=self.key_cols, tracked_cols=self.tracked_cols, type1_cols=self.type1_cols,
                        comment=f"RFB {self.entity}: SCD2 history (valid_from/valid_to in YYYYMM)")


MODELS: dict[str, EntityModel] = {
    "estabelecimentos": EntityModel(
        entity="estabelecimentos", table="estabelecimento_hist",
        key_cols=("cnpj",),
        tracked_cols=("cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial", "nome_fantasia",
                      "situacao_cadastral", "data_situacao_cadastral", "motivo_situacao_cadastral",
                      "nome_cidade_exterior", "pais", "data_inicio_atividade", "cnae_fiscal_principal",
                      "cnae_fiscal_secundaria", "tipo_logradouro", "logradouro", "numero", "complemento", "bairro",
                      "cep", "uf", "municipio", "situacao_especial", "data_situacao_especial"),
        # contacts change often and carry personal data: current value only (type 1), no version churn
        type1_cols=("ddd_1", "telefone_1", "ddd_2", "telefone_2", "ddd_fax", "fax", "correio_eletronico",
                    "cnpj_dv_valido"),
        dedup_order=("data_situacao_cadastral DESC NULLS LAST", "situacao_cadastral"),
        date_cols=("data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial"),
    ),
    "empresas": EntityModel(
        entity="empresas", table="empresa_hist",
        key_cols=("cnpj_basico",),
        tracked_cols=("razao_social", "natureza_juridica", "qualificacao_responsavel", "capital_social",
                      "porte_empresa", "ente_federativo_responsavel"),
        type1_cols=(),
        dedup_order=("capital_social DESC NULLS LAST", "razao_social"),
        date_cols=(),
    ),
    "simples": EntityModel(
        entity="simples", table="simples_hist",
        key_cols=("cnpj_basico",),
        tracked_cols=("opcao_simples", "data_opcao_simples", "data_exclusao_simples", "opcao_mei",
                      "data_opcao_mei", "data_exclusao_mei"),
        type1_cols=(),
        dedup_order=("data_opcao_simples DESC NULLS LAST",),
        date_cols=("data_opcao_simples", "data_exclusao_simples", "data_opcao_mei", "data_exclusao_mei"),
    ),
    "socios": EntityModel(
        entity="socios", table="socio_hist",
        # no clean key at the source (profile: 292 exact duplicates + a few conflicts): a partner is identified by
        # company + type + masked document + name; the rest is tracked
        key_cols=("cnpj_basico", "identificador_socio", "cnpj_cpf_socio", "nome_socio"),
        tracked_cols=("qualificacao_socio", "data_entrada_sociedade", "pais", "representante_legal",
                      "nome_representante", "qualificacao_representante", "faixa_etaria"),
        type1_cols=(),
        dedup_order=("data_entrada_sociedade DESC NULLS LAST", "qualificacao_socio"),
        date_cols=("data_entrada_sociedade",),
    ),
}
DOMAIN_TABLES = {"cnaes": "cnae", "motivos": "motivo_situacao", "municipios": "municipio",
                 "naturezas": "natureza_juridica", "paises": "pais", "qualificacoes": "qualificacao_socio"}


def transform(entity: str, bronze_df):
    """Bronze strings -> typed silver columns (exactly the model's data columns)."""
    from pyspark.sql import functions as F

    b = bronze_df
    if entity == "estabelecimentos":
        return b.select(
            F.concat(code("cnpj_basico", 8), code("cnpj_ordem", 4), code("cnpj_dv", 2)).alias("cnpj"),
            code("cnpj_basico", 8).alias("cnpj_basico"), code("cnpj_ordem", 4).alias("cnpj_ordem"),
            code("cnpj_dv", 2).alias("cnpj_dv"),
            code("identificador_matriz_filial").alias("identificador_matriz_filial"),
            clean_text("nome_fantasia").alias("nome_fantasia"),
            code("situacao_cadastral", 2).alias("situacao_cadastral"),
            rfb_date("data_situacao_cadastral").alias("data_situacao_cadastral"),
            code("motivo_situacao_cadastral", 2).alias("motivo_situacao_cadastral"),
            clean_text("nome_cidade_exterior").alias("nome_cidade_exterior"),
            code("pais", 3).alias("pais"),
            rfb_date("data_inicio_atividade").alias("data_inicio_atividade"),
            code("cnae_fiscal_principal", 7).alias("cnae_fiscal_principal"),
            F.when(F.trim("cnae_fiscal_secundaria") == "", None)
             .otherwise(F.split(F.trim("cnae_fiscal_secundaria"), ",")).alias("cnae_fiscal_secundaria"),
            clean_text("tipo_logradouro").alias("tipo_logradouro"), clean_text("logradouro").alias("logradouro"),
            clean_text("numero").alias("numero"), clean_text("complemento").alias("complemento"),
            clean_text("bairro").alias("bairro"), code("cep", 8).alias("cep"),
            F.upper(code("uf")).alias("uf"), code("municipio", 4).alias("municipio"),
            clean_text("situacao_especial").alias("situacao_especial"),
            rfb_date("data_situacao_especial").alias("data_situacao_especial"),
            code("ddd_1").alias("ddd_1"), code("telefone_1").alias("telefone_1"),
            code("ddd_2").alias("ddd_2"), code("telefone_2").alias("telefone_2"),
            code("ddd_fax").alias("ddd_fax"), code("fax").alias("fax"),
            F.lower(clean_text("correio_eletronico")).alias("correio_eletronico"),
            cnpj_dv_ok("cnpj_basico", "cnpj_ordem", "cnpj_dv").alias("cnpj_dv_valido"),
        )
    if entity == "empresas":
        return b.select(
            code("cnpj_basico", 8).alias("cnpj_basico"), clean_text("razao_social").alias("razao_social"),
            code("natureza_juridica", 4).alias("natureza_juridica"),
            code("qualificacao_responsavel", 2).alias("qualificacao_responsavel"),
            money_br("capital_social").alias("capital_social"), code("porte_empresa", 2).alias("porte_empresa"),
            clean_text("ente_federativo_responsavel").alias("ente_federativo_responsavel"),
        )
    if entity == "simples":
        return b.select(
            code("cnpj_basico", 8).alias("cnpj_basico"),
            flag_sn("opcao_simples").alias("opcao_simples"), rfb_date("data_opcao_simples").alias("data_opcao_simples"),
            rfb_date("data_exclusao_simples").alias("data_exclusao_simples"),
            flag_sn("opcao_mei").alias("opcao_mei"), rfb_date("data_opcao_mei").alias("data_opcao_mei"),
            rfb_date("data_exclusao_mei").alias("data_exclusao_mei"),
        )
    if entity == "socios":
        return b.select(
            code("cnpj_basico", 8).alias("cnpj_basico"), code("identificador_socio").alias("identificador_socio"),
            F.coalesce(code("cnpj_cpf_socio"), F.lit("")).alias("cnpj_cpf_socio"),
            F.coalesce(clean_text("nome_socio"), F.lit("")).alias("nome_socio"),
            code("qualificacao_socio", 2).alias("qualificacao_socio"),
            rfb_date("data_entrada_sociedade").alias("data_entrada_sociedade"), code("pais", 3).alias("pais"),
            code("representante_legal").alias("representante_legal"),
            clean_text("nome_representante").alias("nome_representante"),
            code("qualificacao_representante", 2).alias("qualificacao_representante"),
            code("faixa_etaria").alias("faixa_etaria"),
        )
    raise KeyError(f"no silver transform for {entity}")


def transform_domain(bronze_df):
    from pyspark.sql import functions as F

    return bronze_df.select(code("codigo").alias("codigo"), clean_text("descricao").alias("descricao"),
                            F.col("_ref_month"))


def dedup(df, model: EntityModel):
    """Exact duplicates removed; among rows with the same key and different values, the first of `dedup_order`
    wins and the others are returned as `losers` (to quarantine). Returns (winners, losers, ranked), where `ranked`
    holds the distinct rows with `_rn` (1 = winner).

    ONE shuffle + sort by key: within a key, rows are ordered by `dedup_order` and then by every other column, so the
    order is total (deterministic winner, type-1 columns included) and identical rows are adjacent: a row equal to
    the previous one is an exact duplicate. A `dropDuplicates()` first adds a hash aggregate over all columns plus a
    second shuffle that removes almost nothing (2026-09: 0 of 73.4 M estabelecimentos, 292 of 28.3 M socios)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    sort_cols = [o.split()[0] for o in model.dedup_order]
    order = [_sort_col(o) for o in model.dedup_order] + [
        F.col(c).asc_nulls_last() for c in df.columns if c not in model.key_cols and c not in sort_cols]
    w = Window.partitionBy(*model.key_cols).orderBy(*order)
    row = F.struct(*df.columns)
    flagged = df.select("*", F.row_number().over(w).alias("_rn"),
                        F.lag(row).over(w).eqNullSafe(row).alias("_dup"))
    ranked = flagged.where(~F.col("_dup")).drop("_dup")
    return ranked.where("_rn = 1").drop("_rn"), ranked.where("_rn > 1").drop("_rn"), ranked


def _sort_col(spec: str):
    """'col DESC NULLS LAST' / 'col' -> a Column sort expression."""
    from pyspark.sql import functions as F

    name, *words = spec.split()
    desc = "DESC" in (w.upper() for w in words)
    return F.col(name).desc_nulls_last() if desc else F.col(name).asc_nulls_last()


def bronze_partition(spark, entity: str, ref: RefMonth):
    from pyspark.sql import functions as F

    return spark.table(f"rfb_bronze.{entity}").where(F.col("_ref_month") == ref.yyyymm)
