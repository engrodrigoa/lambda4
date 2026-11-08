{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'mes_coleta',
    on_schema_change = 'append_new_columns',
    post_hook = [
        "create index if not exists ix_fct_preco_diario_mes on {{ this }} (mes_coleta)",
        "create index if not exists ix_fct_preco_diario_data on {{ this }} (data_coleta)",
        "create index if not exists ix_fct_preco_diario_posto_produto on {{ this }} (cnpj, produto, data_coleta)",
        "analyze {{ this }}"
    ]
) }}

-- Fato de preço: um registro por posto x produto x dia de coleta. Os conflitos da silver (mesmo posto/produto/dia
-- com mais de um preço publicado) viram média + mín + máx + qt_precos. Preços suspeitos ficam, marcados
-- (fl_preco_suspeito): os marts de serving os deixam fora das médias.
-- Incremental: reconstrói os meses inteiros que a silver recarregou desde a última carga (mesma unidade da silver).

with silver as (
    select cnpj, produto, data_coleta, mes_coleta, valor_venda, valor_compra, fl_preco_suspeito,
           bandeira, uf, municipio, _source_kind, _ingested_at
    from {{ ref('anp__precos_coleta') }}
    {% if is_incremental() %}
    where mes_coleta in (
        select distinct mes_coleta from {{ ref('anp__precos_coleta') }}
        where _ingested_at > (select coalesce(max(_ingested_at), '1900-01-01') from {{ this }})
    )
    {% endif %}
),

dia as (
    select
        cnpj, produto, data_coleta, mes_coleta,
        round(avg(valor_venda), 4)          as preco_venda,
        min(valor_venda)                    as preco_venda_min,
        max(valor_venda)                    as preco_venda_max,
        count(*)                            as qt_precos,
        round(avg(valor_compra), 4)         as preco_compra,
        bool_or(fl_preco_suspeito)          as fl_preco_suspeito,
        min(bandeira)                       as bandeira_publicada,   -- no GLP: a distribuidora do gás
        min(uf)                             as uf,
        min(municipio)                      as municipio,
        min(_source_kind)                   as _source_kind,
        max(_ingested_at)                   as _ingested_at
    from silver
    group by cnpj, produto, data_coleta, mes_coleta
)

select
    d.data_coleta,
    d.mes_coleta,
    d.cnpj,
    p.posto_sk,                                     -- versão do posto vigente no dia (SCD2)
    d.produto,
    md5(d.uf || '|' || d.municipio)                 as municipio_sk,
    d.uf,
    d.municipio,
    d.bandeira_publicada,
    d.preco_venda,
    d.preco_venda_min,
    d.preco_venda_max,
    d.qt_precos,
    d.qt_precos > 1                                 as fl_conflito_preco,
    d.preco_compra,
    d.fl_preco_suspeito,
    d._source_kind,
    d._ingested_at,
    now()                                           as _dbt_loaded_at
from dia d
left join {{ ref('dim_posto') }} p                  -- left: um dia sem versão vira posto_sk NULL e o teste acusa
       on p.cnpj = d.cnpj
      and d.data_coleta >= p.valid_from
      and d.data_coleta <  p.valid_to
