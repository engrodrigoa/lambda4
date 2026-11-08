-- silver: exactly one source per month x product group (the precedence rule is per month, never per row)
select mes_coleta, product_group,
       count(distinct _source_kind)                           as fontes,
       string_agg(distinct _source_kind, ',' order by _source_kind) as quais
from {{ ref('anp__precos_coleta') }}
group by 1, 2
having count(distinct _source_kind) > 1
