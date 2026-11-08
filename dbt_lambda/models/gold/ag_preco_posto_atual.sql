{#- grants: the Streamlit role anp_app_ro reads only the two serving marts (added to the project's read roles) -#}
{{ config(
    materialized = 'table',
    grants = {'+select': ['anp_app_ro']},
    indexes = [{'columns': ['uf', 'municipio']}, {'columns': ['data_coleta']}],
    post_hook = ["analyze {{ this }}"]
) }}

-- Serving (Streamlit, cards e matriz): o último preço de cada posto x produto e o anterior (variação). A ANP coleta
-- cada posto num dia diferente, então cada linha tem a sua data_coleta: o app decide a idade máxima (janela de
-- recência); o modelo só limita a anp_gold_atual_meses meses para não varrer 20 anos. Sem preços suspeitos.
-- Atributos do posto = versão ATUAL da dim_posto; bandeira = a publicada na linha (no GLP, a distribuidora).

with recentes as (
    select f.*,
           row_number() over (partition by f.cnpj, f.produto order by f.data_coleta desc) as rn,
           count(*)     over (partition by f.cnpj, f.produto)                             as qtd_coletas
    from {{ ref('fct_preco_diario') }} f
    where not f.fl_preco_suspeito
      and f.data_coleta >= (select max(data_coleta) from {{ ref('fct_preco_diario') }})
                           - make_interval(months => {{ var('anp_gold_atual_meses') }})
),

ultimas as (
    select
        cnpj, produto,
        max(data_coleta)        filter (where rn = 1) as data_coleta,
        max(preco_venda)        filter (where rn = 1) as valor_venda,
        max(bandeira_publicada) filter (where rn = 1) as bandeira,
        max(data_coleta)        filter (where rn = 2) as data_coleta_anterior,
        max(preco_venda)        filter (where rn = 2) as valor_venda_anterior,
        max(qtd_coletas)                              as qtd_coletas,
        max(_ingested_at)                             as _source_loaded_at
    from recentes
    where rn <= 2
    group by cnpj, produto
)

select
    p.posto_sk,
    u.cnpj,
    u.produto,
    pr.unidade_medida                               as unidade_de_medida,
    pr.ordem                                        as ordem_produto,
    p.revenda,
    u.bandeira,
    p.tipo_revenda,
    p.regiao                                        as regiao_sigla,
    p.uf,
    p.municipio,
    p.bairro,
    p.endereco,
    p.cep,
    u.data_coleta,
    u.valor_venda,
    u.data_coleta_anterior,
    u.valor_venda_anterior,
    u.valor_venda - u.valor_venda_anterior          as variacao,
    u.qtd_coletas,
    u._source_loaded_at
from ultimas u
join {{ ref('dim_posto') }}   p  on p.cnpj = u.cnpj and p.fl_atual
join {{ ref('dim_produto') }} pr on pr.produto = u.produto
