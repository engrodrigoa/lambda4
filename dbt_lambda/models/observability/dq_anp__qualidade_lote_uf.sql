-- Portão de qualidade por lote e UF (mês x produto x UF): onde está a discrepância quando o lote nacional acusa.
select
    mes_coleta,
    produto,
    uf,
    count(*)                                                                 as linhas,
    count(distinct cnpj)                                                     as postos,
    round(avg(valor_venda), 4)                                               as preco_medio,
    percentile_cont(0.5) within group (order by valor_venda)::numeric(12,4)  as preco_mediana,
    min(valor_venda)                                                         as preco_min,
    max(valor_venda)                                                         as preco_max,
    count(*) filter (where fl_preco_suspeito)                                as linhas_preco_suspeito,
    current_timestamp                                                        as calculado_em
from {{ ref('anp__precos_coleta') }}
group by 1, 2, 3
