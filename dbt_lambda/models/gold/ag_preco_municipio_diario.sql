{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'mes_coleta',
    post_hook = [
        "create index if not exists ix_ag_mun_diario_data on {{ this }} (data_coleta)",
        "create index if not exists ix_ag_mun_diario_local on {{ this }} (uf, municipio, produto)",
        "analyze {{ this }}"
    ]
) }}

-- Serving (Streamlit, aba Histórico): dia de coleta x município x produto. soma_preco_venda + qtd_postos permitem
-- subir para UF/Brasil e para semanas EXATAMENTE (média = soma / quantidade), nunca média de médias.
-- Para no município de propósito: no bairro a amostra por dia é de 1 ou 2 postos. Sem preços suspeitos.

select
    f.data_coleta,
    f.mes_coleta,
    f.uf,
    f.municipio,
    f.produto,
    count(*)                                        as qtd_postos,
    sum(f.preco_venda)                              as soma_preco_venda,
    round(avg(f.preco_venda), 4)                    as preco_medio,
    min(f.preco_venda_min)                          as preco_min,
    max(f.preco_venda_max)                          as preco_max,
    max(f._ingested_at)                             as _ingested_at
from {{ ref('fct_preco_diario') }} f
where not f.fl_preco_suspeito
{% if is_incremental() %}
  and f.mes_coleta in (
      select distinct mes_coleta from {{ ref('fct_preco_diario') }}
      where _ingested_at > (select coalesce(max(_ingested_at), '1900-01-01') from {{ this }})
  )
{% endif %}
group by 1, 2, 3, 4, 5
