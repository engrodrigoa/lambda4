-- por arquivo: linhas na staging = linhas no batch_profile do anp03
-- (batch_month = 0 são as linhas vazias, já filtradas na stg e medidas em assert_bronze_linhas_vazias)
with profile as (
    select file_key, sum(rows) as linhas
    from {{ source('anp_bronze', 'batch_profile') }}
    where produto = '*' and batch_month <> 0
    group by 1
),
stg as (
    select _file_key as file_key, count(*) as linhas
    from {{ ref('stg_anp__precos') }}
    group by 1
)
select coalesce(p.file_key, s.file_key) as file_key,
       p.linhas as linhas_profile,
       s.linhas as linhas_stg
from profile p
full join stg s using (file_key)
where p.linhas is distinct from s.linhas