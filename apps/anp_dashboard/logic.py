"""
Pure Polars transforms of the ANP dashboard (no Streamlit, no database), so they are unit-tested in CI.

Ported from the datalabs portfolio (apps/anp_dashboard), same contract. Inputs are the two anp_gold serving marts:
  * postos    -> anp_gold.ag_preco_posto_atual       (one row per station x product, latest price)
  * historico -> anp_gold.ag_preco_municipio_diario  rolled up by data.py (day x product)
"""
from __future__ import annotations

import datetime as dt

import polars as pl

# Fixed product order: drives the KPI cards, the matrix columns and the series colours
# (colour follows the product, never its rank, so a filter never repaints a line).
PRODUTOS = ["GASOLINA", "GASOLINA ADITIVADA", "ETANOL", "DIESEL S10", "DIESEL", "GNV", "GLP", "DIESEL S50"]
# Categorical slots stepped for a dark surface (#17171d); validated: CVD adjacent dE >= 8.4, contrast >= 3:1.
CORES = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"]
COR_OUTROS = "#8888a0"

NIVEIS = {
    "Estado": ["uf"],
    "Cidade": ["uf", "municipio"],
    "Bairro": ["uf", "municipio", "bairro"],
    "Posto": ["uf", "municipio", "bairro", "revenda", "bandeira", "cnpj", "endereco"],
}


def ordenar_produtos(produtos) -> list[str]:
    """Known products in the fixed order, then any new ANP product alphabetically."""
    conhecidos = [p for p in PRODUTOS if p in set(produtos)]
    return conhecidos + sorted(set(produtos) - set(PRODUTOS))


def rotulo(produto: str) -> str:
    """Display name: title case, acronyms and grades kept (GNV, GLP, S10, S50)."""
    return " ".join(w if w in {"GNV", "GLP", "S10", "S50", "S500"} else w.title() for w in produto.split())


def unidade_curta(unidade: str | None) -> str:
    """ANP unit text -> what follows 'R$/': 'R$ / litro' -> 'litro', 'R$ / 13 kg' -> '13 kg' (GLP bottle),
    'R$ / m³' -> 'm³' (GNV). Missing -> 'litro', the unit of every liquid fuel."""
    if not unidade or "/" not in unidade:
        return "litro"
    return unidade.split("/", 1)[1].strip() or "litro"


def unidades(postos: pl.DataFrame) -> dict[str, str]:
    """Product -> short unit, read from the data (anp_gold.dim_produto via the mart), never hard-coded."""
    if postos.is_empty() or "unidade_de_medida" not in postos.columns:
        return {}
    pares = postos.group_by("produto").agg(pl.col("unidade_de_medida").drop_nulls().mode().first())
    return {p: unidade_curta(u) for p, u in pares.iter_rows()}


def casas_decimais(unidade: str) -> int:
    """Pump prices are quoted to 3 decimals per litre; a 13 kg bottle or a m³ to 2."""
    return 3 if unidade == "litro" else 2


def agrupar_por_unidade(produtos, unidade_de: dict[str, str]) -> list[tuple[str, list[str]]]:
    """Split a product selection by unit, in product order: one chart per unit (one y-axis each),
    so R$ 113 per GLP bottle never shares a scale with R$ 4 per litre of ethanol."""
    grupos: dict[str, list[str]] = {}
    for p in ordenar_produtos(produtos):
        grupos.setdefault(unidade_de.get(p, "litro"), []).append(p)
    return list(grupos.items())


SEM_PRECO = "—"


def precos_como_texto(df: pl.DataFrame, casas_por_coluna: dict[str, int]) -> pl.DataFrame:
    """Display copy of the matrix: 'R$ 6,290' (pt-BR) and '—' when the station does not sell the product.

    Streamlit 1.66 draws a null cell as 'None' and has no placeholder option, so the price columns go as text.
    Text sorts lexically when the reader clicks a header, so the integer part is left-padded with spaces to the
    column's widest value: lexical order == numeric order ('R$  89,99' < 'R$ 112,21')."""
    out = df
    for col, casas in casas_por_coluna.items():
        valores = df[col].drop_nulls()
        largura = len(str(int(valores.max()))) if valores.len() else 1

        def fmt(v: float, casas=casas, largura=largura) -> str:
            inteiro, _, frac = f"{v:.{casas}f}".partition(".")
            return f"R$ {inteiro.rjust(largura)},{frac}"

        out = out.with_columns(pl.col(col).map_elements(fmt, return_dtype=pl.Utf8).fill_null(SEM_PRECO))
    return out


def cor_produto(produto: str) -> str:
    return CORES[PRODUTOS.index(produto)] if produto in PRODUTOS else COR_OUTROS


def data_referencia(postos: pl.DataFrame) -> dt.date | None:
    return postos["data_coleta"].max() if postos.height else None


def aplicar_filtros(postos: pl.DataFrame, uf=None, municipio=None, bairro=None, bandeiras=None,
                    busca: str | None = None) -> pl.DataFrame:
    """Location cascade + brand + free-text search (station name or CNPJ). None = no filter."""
    cond = pl.lit(True)
    if uf:
        cond &= pl.col("uf") == uf
    if municipio:
        cond &= pl.col("municipio") == municipio
    if bairro:
        cond &= pl.col("bairro") == bairro
    if bandeiras:
        cond &= pl.col("bandeira").is_in(list(bandeiras))
    if busca and busca.strip():
        termo = busca.strip().upper()
        digitos = "".join(c for c in termo if c.isdigit())
        hit = pl.col("revenda").str.to_uppercase().str.contains(termo, literal=True)
        if len(digitos) >= 4:
            hit |= pl.col("cnpj").str.contains(digitos, literal=True)
        cond &= hit
    return postos.filter(cond)


def aplicar_janela(postos: pl.DataFrame, dias: int, ref: dt.date | None = None) -> pl.DataFrame:
    """Keep the stations whose latest price is at most `dias` older than the reference date
    (default: the newest collection in the data). ANP surveys each station on its own day, so
    'latest price' without a window would mix this week with a station last seen months ago."""
    ref = ref or data_referencia(postos)
    if ref is None:
        return postos
    return postos.filter(pl.col("data_coleta") >= ref - dt.timedelta(days=dias))


def kpis(postos: pl.DataFrame) -> pl.DataFrame:
    """One row per product: average of the stations' latest prices, range, coverage and the
    average change of each station against its previous collection."""
    if postos.is_empty():
        return pl.DataFrame(schema={"produto": pl.Utf8, "preco_medio": pl.Float64, "preco_min": pl.Float64,
                                    "preco_max": pl.Float64, "qtd_postos": pl.UInt32, "variacao_media": pl.Float64,
                                    "ultima_coleta": pl.Date})
    out = postos.group_by("produto").agg(
        pl.col("valor_venda").mean().alias("preco_medio"),
        pl.col("valor_venda").min().alias("preco_min"),
        pl.col("valor_venda").max().alias("preco_max"),
        pl.col("cnpj").n_unique().alias("qtd_postos"),
        pl.col("variacao").mean().alias("variacao_media"),
        pl.col("data_coleta").max().alias("ultima_coleta"),
    )
    ordem = {p: i for i, p in enumerate(ordenar_produtos(out["produto"].to_list()))}
    return out.sort(pl.col("produto").replace_strict(ordem, return_dtype=pl.Int32))


def matriz(postos: pl.DataFrame, nivel: str, produto_principal: str | None = None) -> pl.DataFrame:
    """Power BI-style matrix: rows at `nivel`, one column per product (average latest price),
    plus stations count and newest collection. Sorted cheapest first on the main product.
    At station level adds `vs_media_cidade` (%) for the main product, like the reference site."""
    chaves = NIVEIS[nivel]
    if postos.is_empty():
        return pl.DataFrame({c: [] for c in chaves})
    produtos = ordenar_produtos(postos["produto"].unique().to_list())

    precos = (postos.group_by(chaves + ["produto"]).agg(pl.col("valor_venda").mean())
              .pivot(on="produto", index=chaves, values="valor_venda"))
    resumo = postos.group_by(chaves).agg(
        pl.col("cnpj").n_unique().alias("qtd_postos"),
        pl.col("data_coleta").max().alias("ultima_coleta"),
    )
    out = resumo.join(precos, on=chaves, how="left").select(chaves + ["qtd_postos", "ultima_coleta"] + produtos)

    principal = produto_principal if produto_principal in produtos else produtos[0]
    if nivel == "Posto":
        media = (postos.filter(pl.col("produto") == principal)
                 .group_by(["uf", "municipio"]).agg(pl.col("valor_venda").mean().alias("_media")))
        out = (out.join(media, on=["uf", "municipio"], how="left")
               .with_columns(((pl.col(principal) / pl.col("_media") - 1) * 100).alias("vs_media_cidade"))
               .drop("_media"))
    return out.sort([principal] + chaves, nulls_last=True)


def historico(diario: pl.DataFrame, granularidade: str = "Dia") -> pl.DataFrame:
    """Roll the daily series (data_coleta, produto, qtd_postos, soma_valor_venda, preco_min, preco_max)
    up to weeks when asked, then compute the exact average (sum / count, never an average of averages)."""
    if diario.is_empty():
        return diario.with_columns(pl.lit(None, pl.Float64).alias("preco_medio"))
    df = diario
    if granularidade == "Semana":
        df = (df.with_columns(pl.col("data_coleta").dt.truncate("1w").alias("data_coleta"))
              .group_by(["data_coleta", "produto"]).agg(
                  pl.col("qtd_postos").sum(), pl.col("soma_valor_venda").sum(),
                  pl.col("preco_min").min(), pl.col("preco_max").max()))
    return (df.with_columns((pl.col("soma_valor_venda") / pl.col("qtd_postos")).alias("preco_medio"))
            .sort(["produto", "data_coleta"]))


def cobertura(postos_filtrados: pl.DataFrame, postos_na_janela: pl.DataFrame) -> dict:
    """How much of the selection the recency window keeps (data health, shown to the reader)."""
    total = postos_filtrados["cnpj"].n_unique() if postos_filtrados.height else 0
    dentro = postos_na_janela["cnpj"].n_unique() if postos_na_janela.height else 0
    return {
        "postos_total": total,
        "postos_na_janela": dentro,
        "pct_na_janela": (dentro / total * 100) if total else 0.0,
        "coleta_mais_antiga": postos_filtrados["data_coleta"].min() if total else None,
        "coleta_mais_recente": postos_filtrados["data_coleta"].max() if total else None,
    }
