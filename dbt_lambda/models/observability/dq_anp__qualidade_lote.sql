-- Portão de qualidade de preço por lote consumido na silver: mês da coleta x produto (Brasil).
-- Mesma ideia do anp_bronze.batch_profile, agora sobre o dado tratado (uma fonte por mês, sem duplicatas):
-- média, mediana, mínimo, máximo, p01/p99, dispersão, preços suspeitos e a variação da mediana contra o
-- mês anterior. Os testes julgam só os meses recentes (vars anp_gate_*): o histórico fica visível no Grafana,
-- sem alerta permanente por uma alta real de 2022.

with s as (
    select mes_coleta, produto, product_group, _source_kind, _file_key,
           cnpj, uf, municipio, bandeira, valor_venda, fl_preco_suspeito, fl_conflito_preco
    from {{ ref('anp__precos_coleta') }}
),

lote as (
    select
        mes_coleta,
        produto,
        min(product_group)                                                       as product_group,
        min(_source_kind)                                                        as fonte,
        string_agg(distinct _file_key, ', ' order by _file_key)                  as arquivos,
        count(*)                                                                 as linhas,
        count(distinct cnpj)                                                     as postos,
        count(distinct uf)                                                       as ufs,
        count(distinct (uf, municipio))                                          as municipios,
        count(distinct bandeira)                                                 as bandeiras,
        round(avg(valor_venda), 4)                                               as preco_medio,
        percentile_cont(0.5)  within group (order by valor_venda)::numeric(12,4) as preco_mediana,
        min(valor_venda)                                                         as preco_min,
        max(valor_venda)                                                         as preco_max,
        percentile_cont(0.01) within group (order by valor_venda)::numeric(12,4) as preco_p01,
        percentile_cont(0.99) within group (order by valor_venda)::numeric(12,4) as preco_p99,
        round(stddev_samp(valor_venda), 4)                                       as preco_desvio,
        -- o que a gold vai usar: médias sem os preços suspeitos
        round(avg(valor_venda) filter (where not fl_preco_suspeito), 4)          as preco_medio_limpo,
        count(*) filter (where fl_preco_suspeito)                                as linhas_preco_suspeito,
        count(*) filter (where fl_conflito_preco)                                as linhas_conflito
    from s
    group by 1, 2
)

select
    l.*,
    round(100 * l.preco_desvio / nullif(l.preco_medio, 0), 2)                    as cv_pct,
    round(l.linhas_preco_suspeito::numeric / l.linhas, 6)                        as share_preco_suspeito,
    lag(l.mes_coleta)    over w                                                  as mes_anterior,
    lag(l.preco_mediana) over w                                                  as mediana_mes_anterior,
    -- só compara meses consecutivos (produto que some e volta não gera variação falsa)
    case when lag(l.mes_coleta) over w = (l.mes_coleta - interval '1 month')::date
         then round(100 * (l.preco_mediana / nullif(lag(l.preco_mediana) over w, 0) - 1), 2)
    end                                                                          as var_mediana_pct,
    case when lag(l.mes_coleta) over w = (l.mes_coleta - interval '1 month')::date
         then round(100 * (l.postos::numeric / nullif(lag(l.postos) over w, 0) - 1), 2)
    end                                                                          as var_postos_pct,
    l.mes_coleta >= (date_trunc('month', current_date)
                     - make_interval(months => {{ var('anp_gate_janela_meses') }}))::date as fl_mes_no_portao,
    current_timestamp                                                            as calculado_em
from lote l
window w as (partition by l.produto order by l.mes_coleta)
