{{ config(tags=['gold']) }}
-- o diário por município soma exatamente as linhas e os preços da fato sem suspeitos (o app agrega a partir dele)
with fato as (
    select count(*) as linhas, round(sum(preco_venda), 2) as soma
    from {{ ref('fct_preco_diario') }} where not fl_preco_suspeito
),
mart as (
    select sum(qtd_postos) as linhas, round(sum(soma_preco_venda), 2) as soma
    from {{ ref('ag_preco_municipio_diario') }}
)
select f.linhas as linhas_fato, m.linhas as linhas_mart, f.soma as soma_fato, m.soma as soma_mart
from fato f cross join mart m
where f.linhas <> m.linhas or f.soma <> m.soma
