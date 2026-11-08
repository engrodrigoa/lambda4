"""
Lambda4 · ANP — analytics layer over the anp_gold serving marts (Streamlit). Ported from the datalabs portfolio
(apps/anp_dashboard), same parameters and visual contract.

    ~/.envs/env-app/bin/streamlit run apps/anp_dashboard/app.py --server.port 8501
    sudo systemctl start anp-dashboard          (infra/systemd/anp-dashboard.service, http://localhost:8501)

Two questions, two marts:
  * "how much does it cost now, here?"   -> matrix UF -> city -> bairro -> station (anp_gold.ag_preco_posto_atual)
  * "how did it move?"                   -> history by collection day, down to the city (ag_preco_municipio_diario)
"""
from __future__ import annotations

import datetime as dt
import html

import altair as alt
import data
import logic
import polars as pl
import streamlit as st

st.set_page_config(page_title="Preço dos combustíveis · Lambda4", page_icon="⛽", layout="wide")

# Tokens taken from the reference site (dark, green accent); series colours live in logic.CORES.
st.markdown("""
<style>
:root { --surface-1:#17171d; --surface-2:#1e1e26; --border:#2a2a35; --text-1:#e8e8f0; --text-2:#8888a0;
        --text-3:#5a5a70; --accent:#1d9e75; --up:#e24b4a; --down:#1d9e75; }
.block-container { padding-top: 2rem; max-width: 1400px; }
.dl-sub { color: var(--text-2); font-size: .9rem; margin: -.6rem 0 1rem 0; }
.dl-grid { display:grid; grid-template-columns: repeat(auto-fill, minmax(190px, 1fr)); gap:12px;
           margin: .4rem 0 1.2rem 0; }
.dl-card { background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; }
.dl-card.sel { border-color: var(--accent); background: rgba(29,158,117,.06); }
.dl-card .nome { color: var(--text-2); font-size: .75rem; letter-spacing: .06em; text-transform: uppercase;
                 display:flex; align-items:center; gap:6px; }
.dl-card .dot { width:8px; height:8px; border-radius:50%; display:inline-block; }
.dl-card .preco { color: var(--text-1); font-size: 1.7rem; font-weight: 700; line-height: 1.2; margin-top: 4px; }
.dl-card .un { color: var(--text-3); font-size: .75rem; }
.dl-card .faixa { color: var(--text-2); font-size: .8rem; margin-top: 6px; }
.dl-card .faixa span { white-space: nowrap; }
.dl-card .rodape { color: var(--text-3); font-size: .75rem; margin-top: 4px; }
.dl-up { color: var(--up); } .dl-down { color: var(--down); }
.dl-badge { display:inline-block; border:1px solid rgba(29,158,117,.35); background: rgba(29,158,117,.08);
            color: var(--text-1); border-radius: 6px; padding: 4px 10px; font-size: .85rem; }
</style>
""", unsafe_allow_html=True)


def brl(v, casas: int = 3) -> str:
    if v is None:
        return "—"
    return f"R$ {v:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def dmy(d) -> str:
    return d.strftime("%d/%m/%Y") if d else "—"


def grafico(h: pl.DataFrame, prods: list[str], unidade: str, gran: str):
    """One unit = one y-axis. Min-max band only when a single series is drawn."""
    pdf = h.with_columns(pl.col("data_coleta").cast(pl.Datetime)).to_pandas()  # Altair speaks pandas
    dominio = logic.ordenar_produtos(prods)
    casas = logic.casas_decimais(unidade)
    cor = alt.Color("produto:N", title=None, scale=alt.Scale(
        domain=dominio, range=[logic.cor_produto(p) for p in dominio]),
        legend=alt.Legend(orient="top", labelColor="#e8e8f0") if len(dominio) > 1 else None)
    x = alt.X("data_coleta:T", title=None, axis=alt.Axis(format="%d/%m", labelColor="#8888a0",
                                                         gridColor="#2a2a35", domainColor="#2a2a35"))
    y = alt.Y("preco_medio:Q", title=f"R$/{unidade}", scale=alt.Scale(zero=False),
              axis=alt.Axis(labelColor="#8888a0", titleColor="#8888a0", gridColor="#2a2a35",
                            domainColor="#2a2a35", format=f".{min(casas, 2)}f"))
    base = alt.Chart(pdf).encode(x=x)
    camadas = []
    if len(dominio) == 1:  # one series: min-max band behind the average, like the reference site
        camadas.append(base.mark_area(opacity=0.14).encode(y="preco_min:Q", y2="preco_max:Q", color=cor))
    linhas = base.mark_line(strokeWidth=2, interpolate="monotone",
                            point=alt.OverlayMarkDef(size=18, filled=True) if gran == "Semana" else False
                            ).encode(y=y, color=cor)
    hover = alt.selection_point(fields=["data_coleta"], nearest=True, on="pointerover", empty=False)
    rotulo_data = "Semana de" if gran == "Semana" else "Data"
    pontos = base.mark_point(size=70, filled=True, stroke="#17171d", strokeWidth=2).encode(
        y="preco_medio:Q", color=cor,
        opacity=alt.condition(hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("data_coleta:T", title=rotulo_data, format="%d/%m/%Y"),
                 alt.Tooltip("produto:N", title="Produto"),
                 alt.Tooltip("preco_medio:Q", title="Média", format=f".{casas}f"),
                 alt.Tooltip("preco_min:Q", title="Mín", format=f".{casas}f"),
                 alt.Tooltip("preco_max:Q", title="Máx", format=f".{casas}f"),
                 alt.Tooltip("qtd_postos:Q", title="Coletas")]).add_params(hover)
    regua = base.mark_rule(color="#5a5a70").encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0))).transform_filter(hover)
    camadas += [linhas, regua, pontos]
    return (alt.layer(*camadas).properties(height=340, background="transparent")
            .configure_view(stroke=None).configure_legend(symbolType="stroke"))


# ------------------------------------------------------------------ data (cached; one short socket connection per read)
@st.cache_data(ttl=600, show_spinner="Lendo a gold da ANP…")
def postos_df() -> pl.DataFrame:
    with data.connect() as conn:
        return data.carregar_postos(conn)


@st.cache_data(ttl=600, show_spinner=False)
def historico_df(produtos: tuple[str, ...], desde: dt.date, uf: str | None, municipio: str | None) -> pl.DataFrame:
    with data.connect() as conn:
        return data.carregar_historico(conn, list(produtos), desde, uf, municipio)


try:
    postos = postos_df()
except Exception as exc:  # noqa: BLE001 - shown to the reader: role/peer map missing, Postgres down, no marts
    st.error(f"Não foi possível ler a gold da ANP: {exc}")
    st.info("Confira: Postgres no ar; papel e peer map do app aplicados (`infra/postgres/anp_app_role.sql` + "
            "`pg_ident`/`pg_hba`, ver apps/anp_dashboard/README.md); marts construídos pelo DAG `anp_dbt_gold` "
            "(com os grants do dbt para `anp_app_ro`).")
    st.stop()

if postos.is_empty():
    st.warning("anp_gold.ag_preco_posto_atual está vazia: rode o DAG anp_dbt_gold.")
    st.stop()

ref = logic.data_referencia(postos)
produtos_todos = logic.ordenar_produtos(postos["produto"].unique().to_list())
unidade_de = logic.unidades(postos)            # GLP is quoted per 13 kg bottle, GNV per m³


def un(p: str) -> str:
    return unidade_de.get(p, "litro")


def preco(p: str, v) -> str:
    return brl(v, logic.casas_decimais(un(p)))

# ------------------------------------------------------------------ filters (sidebar, cascading)
with st.sidebar:
    st.markdown("### Filtros")
    produto = st.selectbox("Combustível principal", produtos_todos,
                           help="Ordena a matriz, destaca o card e é o padrão do histórico.")
    ufs = sorted(postos["uf"].unique().to_list())
    uf = st.selectbox("Estado", ["Brasil"] + ufs, index=(ufs.index("GO") + 1) if "GO" in ufs else 0)
    uf = None if uf == "Brasil" else uf

    base_uf = logic.aplicar_filtros(postos, uf=uf)
    cidades = sorted(base_uf["municipio"].unique().to_list()) if uf else []
    municipio = st.selectbox("Cidade", ["Todas"] + cidades, disabled=not uf)
    municipio = None if municipio == "Todas" else municipio

    base_mun = logic.aplicar_filtros(base_uf, municipio=municipio)
    bairros = sorted(base_mun["bairro"].unique().to_list()) if municipio else []
    bairro = st.selectbox("Bairro", ["Todos"] + bairros, disabled=not municipio)
    bairro = None if bairro == "Todos" else bairro

    bandeiras = st.multiselect("Bandeira", sorted(postos["bandeira"].unique().to_list()), placeholder="Todas")
    busca = st.text_input("Posto (nome ou CNPJ)", placeholder="ex.: São Francisco")
    janela = st.select_slider("Janela de recência (dias)", options=[7, 15, 30, 60, 90], value=30,
                              help="Só entram postos cujo último preço tem no máximo N dias "
                                   f"em relação à coleta mais recente da ANP ({dmy(ref)}).")

filtrado = logic.aplicar_filtros(postos, uf, municipio, bairro, bandeiras, busca)
atual = logic.aplicar_janela(filtrado, janela, ref)
cob = logic.cobertura(filtrado, atual)

# ------------------------------------------------------------------ header
local = " · ".join(x.title() for x in [bairro, municipio] if x) or ""
local = f"{local} — {uf}" if local else (uf or "Brasil")
st.markdown(f"## Preço do combustível em {html.escape(local)}")
carga = postos["_source_loaded_at"].max()
st.markdown(f"<div class='dl-sub'>Dados ANP até <b>{dmy(ref)}</b> · último preço de cada posto nos "
            f"últimos {janela} dias · {cob['postos_na_janela']:,} postos · carga em "
            f"{carga:%d/%m/%Y %H:%M}</div>".replace(",", "."), unsafe_allow_html=True)
if (dt.date.today() - ref).days > 10:
    st.warning(f"A coleta mais recente é de {dmy(ref)} ({(dt.date.today() - ref).days} dias): "
               "a carga semanal da ANP pode estar atrasada (veja o board ANP no Grafana).")

# ------------------------------------------------------------------ KPI cards (one per product)
k = logic.kpis(atual)
cards = []
for r in k.iter_rows(named=True):
    var = r["variacao_media"]
    var_html = ""
    if var is not None and abs(var) >= 0.0005:
        cls, seta = ("dl-up", "▲") if var > 0 else ("dl-down", "▼")
        var_html = f" · <span class='{cls}'>{seta} {preco(r['produto'], abs(var))}</span>"
    cards.append(
        f"<div class='dl-card{' sel' if r['produto'] == produto else ''}'>"
        f"<div class='nome'><span class='dot' style='background:{logic.cor_produto(r['produto'])}'></span>"
        f"{html.escape(r['produto'])}</div>"
        f"<div class='preco'>{preco(r['produto'], r['preco_medio']).replace('R$ ', '')}</div>"
        f"<div class='un'>R$/{html.escape(un(r['produto']))} · média</div>"
        f"<div class='faixa'><span>Mín {preco(r['produto'], r['preco_min'])}</span> · "
        f"<span>Máx {preco(r['produto'], r['preco_max'])}</span></div>"
        f"<div class='rodape'>{r['qtd_postos']} postos{var_html}</div></div>")
if cards:
    st.markdown(f"<div class='dl-grid'>{''.join(cards)}</div>", unsafe_allow_html=True)
else:
    st.info("Nenhum posto com preço dentro da janela para esse filtro. Aumente a janela ou abra o filtro.")

aba_matriz, aba_hist, aba_dados = st.tabs(["Matriz de preços", "Histórico", "Sobre os dados"])

# ------------------------------------------------------------------ matrix
with aba_matriz:
    padrao = "Posto" if bairro else ("Bairro" if municipio else ("Cidade" if uf else "Estado"))
    # key per filter depth: drilling down in the sidebar resets the level to the next one down,
    # while the reader can still open another level at the same depth
    nivel = st.segmented_control("Abrir por", list(logic.NIVEIS), default=padrao, key=f"nivel_{padrao}") or padrao
    m = logic.matriz(atual, nivel, produto)
    if m.is_empty():
        st.info("Sem dados para os filtros escolhidos.")
    else:
        rotulos = {"uf": "UF", "municipio": "Cidade", "bairro": "Bairro", "revenda": "Posto", "bandeira": "Bandeira",
                   "cnpj": "CNPJ", "endereco": "Endereço"}
        cfg = {c: st.column_config.TextColumn(rotulos[c]) for c in logic.NIVEIS[nivel]}
        cfg["qtd_postos"] = st.column_config.NumberColumn("Postos", format="%d")
        cfg["ultima_coleta"] = st.column_config.DateColumn("Última coleta", format="DD/MM/YYYY")
        cols_prod = logic.ordenar_produtos([c for c in m.columns if c in produtos_todos])
        for p in cols_prod:
            sufixo = "" if un(p) == "litro" else f" (R$/{un(p)})"
            cfg[p] = st.column_config.TextColumn(logic.rotulo(p) + sufixo, alignment="right")
        exibicao = logic.precos_como_texto(m, {p: logic.casas_decimais(un(p)) for p in cols_prod})
        if "vs_media_cidade" in m.columns:
            cfg["vs_media_cidade"] = st.column_config.Column(
                f"vs média ({logic.rotulo(produto)})",
                help="Preço do posto contra a média da cidade no combustível principal.")
        # numeric column kept numeric (sorts right); the Styler only draws it in pt-BR ("+6,4%")
        tabela = exibicao.to_pandas().style.format(
            {c: (lambda v: f"{v:+.1f}%".replace(".", ",")) for c in ["vs_media_cidade"] if c in exibicao.columns},
            na_rep=logic.SEM_PRECO)
        st.dataframe(tabela, column_config=cfg, hide_index=True, width="stretch",
                     height=min(560, 38 + 35 * m.height))
        st.caption(f"{m.height:,} linhas · média do último preço de cada posto · ordenado pelo mais barato em "
                   f"{logic.rotulo(produto)} · — = posto sem esse combustível na janela · clique no cabeçalho "
                   "para reordenar".replace(",", "."))
        st.download_button("Baixar CSV", m.write_csv(), file_name=f"anp_matriz_{nivel.lower()}_{ref}.csv",
                           mime="text/csv")

# ------------------------------------------------------------------ history (down to the city)
with aba_hist:
    escopo = municipio.title() + f" — {uf}" if municipio else (uf or "Brasil")
    c1, c2, c3 = st.columns([3, 1.3, 1.3])
    sel = c1.multiselect("Combustíveis", produtos_todos, default=[produto], max_selections=4)
    # Semana by default: ANP surveys a different group of stations on each weekday (measured, GO ethanol, 180 days:
    # 1,687 collections on Mondays at R$ 4.299 vs 25 on Fridays at R$ 3.856), so the DAILY average mostly shows
    # which stations were surveyed that day, not a price change.
    gran = c2.segmented_control("Granularidade", ["Semana", "Dia"], default="Semana", key="gran",
                                help="Dia mostra também qual grupo de postos a ANP coletou naquele dia "
                                     "(segunda tem muito mais coletas que sexta): use para auditar, não para "
                                     "ler tendência.") or "Semana"
    periodo = c3.selectbox("Período", ["90 dias", "180 dias", "1 ano", "Tudo"], index=1)
    dias = {"90 dias": 90, "180 dias": 180, "1 ano": 365, "Tudo": 365 * 30}[periodo]
    if bairro or busca or bandeiras:
        st.caption("O histórico vai até o nível de cidade: filtros de bairro, posto e bandeira não se aplicam aqui.")

    if not sel:
        st.info("Escolha ao menos um combustível.")
    else:
        h = logic.historico(historico_df(tuple(sel), ref - dt.timedelta(days=dias), uf, municipio), gran)
        if h.is_empty():
            st.info("Sem histórico para esse escopo.")
        else:
            st.markdown(f"**Preço médio por {'dia de coleta' if gran == 'Dia' else 'semana'} — {escopo}**")
            grupos = logic.agrupar_por_unidade(sel, unidade_de)
            if len(grupos) > 1:
                st.caption("Unidades diferentes (ex.: GLP por botijão de 13 kg) ficam em gráficos separados: "
                           "um eixo por unidade.")
            for unidade, prods in grupos:
                parte = h.filter(pl.col("produto").is_in(prods))
                if parte.is_empty():
                    continue
                st.altair_chart(grafico(parte, prods, unidade, gran), width="stretch")

            dominio = logic.ordenar_produtos(sel)
            resumo = (h.group_by("produto").agg(pl.col("preco_medio").first().alias("inicio"),
                                                 pl.col("preco_medio").last().alias("fim"))
                      .with_columns(((pl.col("fim") / pl.col("inicio") - 1) * 100).alias("pct"))
                      .sort(pl.col("produto").replace_strict({p: i for i, p in enumerate(dominio)},
                                                             return_dtype=pl.Int32)))
            cols = st.columns(len(resumo))
            for col, r in zip(cols, resumo.iter_rows(named=True), strict=True):
                col.metric(logic.rotulo(r["produto"]), preco(r["produto"], r["fim"]),
                           f"{r['pct']:+.1f}% no período".replace(".", ","), delta_color="inverse")
            with st.expander("Ver dados da série"):
                st.dataframe(h.select(["data_coleta", "produto", "preco_medio", "preco_min", "preco_max",
                                       "qtd_postos"]), hide_index=True, width="stretch")

# ------------------------------------------------------------------ data health (observability for the reader)
with aba_dados:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Postos no filtro", f"{cob['postos_total']:,}".replace(",", "."))
    c2.metric("Dentro da janela", f"{cob['pct_na_janela']:.0f}%", f"{cob['postos_na_janela']} postos",
              delta_color="off")
    c3.metric("Coleta mais recente", dmy(cob["coleta_mais_recente"]))
    c4.metric("Coleta mais antiga (último preço)", dmy(cob["coleta_mais_antiga"]))
    st.markdown(
        "- **Último preço por posto**: a ANP coleta cada posto num dia diferente da semana; a matriz usa a coleta "
        "mais recente de cada posto × combustível, limitada pela *janela de recência*.\n"
        "- **Média** = média simples dos postos (cada posto pesa 1), não ponderada por volume vendido.\n"
        "- **Histórico**: média das coletas da semana (ou do dia) no escopo, somadas exatamente a partir do "
        "município (`soma / quantidade`), nunca média de médias. A ANP coleta grupos diferentes de postos em cada "
        "dia útil, então a série **diária** oscila com a composição da amostra; a semanal é a leitura de tendência.\n"
        "- **Unidades** vêm da ANP: combustíveis líquidos em R$/litro, GLP por botijão de 13 kg, GNV por m³.\n"
        "- **Preços suspeitos** (fora de 0,5×–2× da mediana do mês × produto × UF, ex.: gasolina a R$ 99/litro) "
        "ficam fora de todas as médias; continuam marcados na silver e na fato.\n"
        "- **Linhagem**: portal ANP → `anp_bronze.precos_raw` (Python/Polars) → `anp_silver.anp__precos_coleta` "
        "(dbt: uma fonte por mês, sem duplicatas) → `anp_gold.fct_preco_diario` → "
        "`anp_gold.ag_preco_posto_atual` / `anp_gold.ag_preco_municipio_diario` (testes de reconciliação em "
        "cada passo; saúde no Grafana, board *ANP · Transformações dbt*).\n"
        "- Fonte: ANP — Levantamento de Preços de Combustíveis (dados abertos).")
