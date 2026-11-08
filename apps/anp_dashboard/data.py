"""
Data access of the ANP dashboard: reads ONLY the two anp_gold serving marts, as the read-only role anp_app_ro.

No password anywhere: the app runs on the server as the OS user `lambda` and connects over the local socket as
`anp_app_ro` through a peer map (pg_ident `anp_app`: lambda -> anp_app_ro; infra/postgres/anp_app_role.sql). That
role can read the two marts and nothing else (USAGE on anp_gold, SELECT granted by dbt on those two models only).
Configuration: ANP_APP_DSN (default "dbname=lambda host=/var/run/postgresql user=anp_app_ro").
Independent of pipelines/commons on purpose, so the app can be deployed on its own.
Every value that comes from the UI is a bind parameter.
"""
from __future__ import annotations

import datetime as dt
import os

import polars as pl
import psycopg

DEFAULT_DSN = "dbname=lambda host=/var/run/postgresql user=anp_app_ro application_name=anp_dashboard"

SQL_POSTOS = """
SELECT uf, municipio, bairro, revenda, bandeira, cnpj, endereco, produto, unidade_de_medida,
       data_coleta, valor_venda, data_coleta_anterior, valor_venda_anterior, variacao,
       _source_loaded_at
FROM anp_gold.ag_preco_posto_atual
"""

# Scope = Brasil (both NULL), a UF, or a city. Never below the city (see the mart's header).
SQL_HISTORICO = """
SELECT data_coleta, produto,
       SUM(qtd_postos)::bigint            AS qtd_postos,
       SUM(soma_preco_venda)::float8      AS soma_valor_venda,
       MIN(preco_min)::float8             AS preco_min,
       MAX(preco_max)::float8             AS preco_max
FROM anp_gold.ag_preco_municipio_diario
WHERE produto = ANY(%(produtos)s)
  AND data_coleta >= %(desde)s
  AND (%(uf)s::text IS NULL OR uf = %(uf)s)
  AND (%(municipio)s::text IS NULL OR municipio = %(municipio)s)
GROUP BY data_coleta, produto
ORDER BY data_coleta, produto
"""


def dsn() -> str:
    return os.getenv("ANP_APP_DSN", DEFAULT_DSN)


def connect(conninfo: str | None = None) -> psycopg.Connection:
    # read-only session: even a mistake in this file cannot write
    return psycopg.connect(conninfo or dsn(), autocommit=True, options="-c default_transaction_read_only=on")


def _frame(conn: psycopg.Connection, sql: str, params: dict | None = None) -> pl.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [c.name for c in cur.description]
        rows = cur.fetchall()
    if not rows:
        return pl.DataFrame({c: [] for c in cols})
    return pl.DataFrame(rows, schema=cols, orient="row", infer_schema_length=None)


def carregar_postos(conn: psycopg.Connection) -> pl.DataFrame:
    """The whole 'current picture' (~45k station x product rows): small enough to filter in memory."""
    df = _frame(conn, SQL_POSTOS)
    if df.is_empty():
        return df
    return df.with_columns(pl.col(["valor_venda", "valor_venda_anterior", "variacao"]).cast(pl.Float64))


def carregar_historico(conn: psycopg.Connection, produtos: list[str], desde: dt.date,
                       uf: str | None = None, municipio: str | None = None) -> pl.DataFrame:
    return _frame(conn, SQL_HISTORICO,
                  {"produtos": list(produtos), "desde": desde, "uf": uf, "municipio": municipio})
