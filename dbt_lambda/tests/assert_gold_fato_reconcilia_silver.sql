{{ config(tags=['gold']) }}
-- por mês: toda linha da silver está na fato (sum(qt_precos) = linhas da silver), nem perde nem duplica
with silver as (
    select mes_coleta, count(*) as linhas from {{ ref('anp__precos_coleta') }} group by 1
),
fato as (
    select mes_coleta, sum(qt_precos) as linhas from {{ ref('fct_preco_diario') }} group by 1
)
select coalesce(s.mes_coleta, f.mes_coleta) as mes_coleta, s.linhas as linhas_silver, f.linhas as linhas_fato
from silver s
full join fato f using (mes_coleta)
where s.linhas is distinct from f.linhas
