{{ config(severity='warn') }}
-- o arquivo semanal mais novo carregado de cada grupo tem no máximo anp_semanal_max_dias: a janela móvel de 4
-- semanas da ANP só recupera uma semana perdida enquanto ela ainda está dentro da janela
select product_group, max(period_end) as ultimo_semanal,
       current_date - max(period_end)  as dias
from {{ source('ctrl', 'source_file') }}
where pipeline = 'anp' and source_kind = 'semanal' and bronze_status = 'LOADED'
group by 1
having current_date - max(period_end) > {{ var('anp_semanal_max_dias') }}
