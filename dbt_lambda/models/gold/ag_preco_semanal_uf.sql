-- Agregado semanal (Grafana / BI): semana ISO x UF x produto, exato a partir do diário por município.
select
    date_trunc('week', data_coleta)::date           as semana,
    uf,
    produto,
    sum(qtd_postos)                                 as qtd_coletas,
    round(sum(soma_preco_venda) / sum(qtd_postos), 4) as preco_medio,
    min(preco_min)                                  as preco_min,
    max(preco_max)                                  as preco_max,
    count(distinct municipio)                       as qtd_municipios,
    max(_ingested_at)                               as _ingested_at
from {{ ref('ag_preco_municipio_diario') }}
group by 1, 2, 3
