"""Silver typing/dedup and gold change events on local Spark, with bronze-shaped synthetic rows."""
import datetime as dt

import pytest

from pipelines.commons import scd2
from pipelines.commons.refmonth import RefMonth
from pipelines.rfb import rfb05_gold
from pipelines.rfb.contract import COLUMNS
from pipelines.rfb.silver_model import MODELS, dedup, transform

pytestmark = pytest.mark.spark
M1, M2 = RefMonth(2026, 8), RefMonth(2026, 9)


def bronze(spark, entity, rows):
    cols = COLUMNS[entity]
    data = [tuple(r.get(c, "") for c in cols) for r in rows]
    return spark.createDataFrame(data, ", ".join(f"{c} string" for c in cols))


def estab(**kw):
    base = {"cnpj_basico": "11222333", "cnpj_ordem": "0001", "cnpj_dv": "81", "identificador_matriz_filial": "1",
            "situacao_cadastral": "02", "data_situacao_cadastral": "20200101", "data_inicio_atividade": "20100515",
            "cnae_fiscal_principal": "6201501", "uf": "SP", "municipio": "7107", "cep": "1310100"}
    return {**base, **kw}


def test_estabelecimento_typing(spark, lake):
    df = bronze(spark, "estabelecimentos", [
        estab(nome_fantasia="LOJA\nCENTRO", logradouro="  RUA  ASUNCI\x1aN ", data_situacao_cadastral="0",
              cnae_fiscal_secundaria="4751201,4752100", correio_eletronico="A@B.COM"),
        estab(cnpj_basico="12ABC345", cnpj_ordem="01DE", cnpj_dv="35", data_inicio_atividade="20251301"),
    ])
    rows = {r["cnpj"]: r for r in transform("estabelecimentos", df).collect()}
    a, b = rows["11222333000181"], rows["12ABC34501DE35"]
    assert a["nome_fantasia"] == "LOJA CENTRO" and a["logradouro"] == "RUA ASUNCI?N"
    assert a["data_situacao_cadastral"] is None and a["data_inicio_atividade"] == dt.date(2010, 5, 15)
    assert a["cep"] == "01310100" and a["cnae_fiscal_secundaria"] == ["4751201", "4752100"]
    assert a["correio_eletronico"] == "a@b.com" and a["cnpj_dv_valido"] is True
    assert b["cnpj_dv_valido"] is True                      # alphanumeric CNPJ, official example 12.ABC.345/01DE-35
    assert b["data_inicio_atividade"] is None               # invalid date -> NULL (counted by the task)


def test_empresa_money_and_dedup(spark, lake):
    df = bronze(spark, "empresas", [
        {"cnpj_basico": "08314885", "razao_social": "X LTDA", "capital_social": "1000,50", "porte_empresa": "1"},
        {"cnpj_basico": "08314885", "razao_social": "X LTDA", "capital_social": "1000,50", "porte_empresa": "1"},
        {"cnpj_basico": "08314885", "razao_social": "X LTDA", "capital_social": "0,00", "porte_empresa": "1"},
        {"cnpj_basico": "00000001", "razao_social": "Y SA", "capital_social": "", "porte_empresa": ""},
    ])
    typed = transform("empresas", df)
    winners, losers, ranked = dedup(typed, MODELS["empresas"])
    assert ranked.count() == 3                               # one exact duplicate dropped
    w = {r["cnpj_basico"]: r for r in winners.collect()}
    assert str(w["08314885"]["capital_social"]) == "1000.50"  # dedup_order: highest capital wins
    assert w["08314885"]["porte_empresa"] == "01" and w["00000001"]["porte_empresa"] is None
    assert losers.count() == 1


def test_change_events_from_history(spark, lake):
    model = MODELS["estabelecimentos"]
    spec = model.spec()
    m1 = transform("estabelecimentos", bronze(spark, "estabelecimentos", [
        estab(nome_fantasia="A"), estab(cnpj_ordem="0002", cnpj_dv="62", nome_fantasia="FILIAL")]))
    m2 = transform("estabelecimentos", bronze(spark, "estabelecimentos", [
        estab(nome_fantasia="B", situacao_cadastral="08"),          # 2 attributes changed
        estab(cnpj_basico="99999999", nome_fantasia="NOVA")]))      # new; the filial disappeared
    scd2.ensure_table(spark, spec, m1.schema)
    scd2.apply_month(spark, spec, m1, M1, "r1")
    events_m1, baseline = rfb05_gold.change_events(spark, "estabelecimentos", M1)
    assert baseline is True and events_m1.count() == 0      # first month = baseline, no "inclusion" flood

    scd2.apply_month(spark, spec, m2, M2, "r2")
    ev, baseline = rfb05_gold.change_events(spark, "estabelecimentos", M2)
    got = sorted((r["tipo_evento"], r["atributo"], r["valor_anterior"], r["valor_novo"]) for r in ev.collect())
    assert baseline is False
    assert got == sorted([("ALTERACAO", "nome_fantasia", "A", "B"), ("ALTERACAO", "situacao_cadastral", "02", "08"),
                          ("EXCLUSAO", None, None, None), ("INCLUSAO", None, None, None)])


def test_dedup_estabelecimentos_with_array_and_dates(spark, lake):
    df = bronze(spark, "estabelecimentos", [
        estab(cnae_fiscal_secundaria="4751201,4752100", data_situacao_cadastral="20200101"),
        estab(cnae_fiscal_secundaria="4751201,4752100", data_situacao_cadastral="20200101"),   # exact duplicate
        estab(cnae_fiscal_secundaria="9999999", data_situacao_cadastral="20240101"),           # conflict, newer
    ])
    winners, losers, ranked = dedup(transform("estabelecimentos", df), MODELS["estabelecimentos"])
    assert ranked.count() == 2 and losers.count() == 1
    w = winners.collect()
    assert len(w) == 1 and w[0]["cnae_fiscal_secundaria"] == ["9999999"]   # newest data_situacao wins
