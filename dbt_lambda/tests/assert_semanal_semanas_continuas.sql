{{ config(severity='warn') }}
-- no arquivo semanal mais novo de cada grupo, toda semana entre a primeira e a última coleta tem coletas
-- (um buraco no meio = semana que a ANP não publicou ou que a carga perdeu)
with ultimo as (
    select distinct on (product_group) product_group, file_key
    from {{ source('ctrl', 'source_file') }}
    where pipeline = 'anp' and source_kind = 'semanal' and bronze_status = 'LOADED'
    order by product_group, period_end desc
),
coletas as (
    select s._file_key, date_trunc('week', s.data_coleta)::date as semana, count(*) as linhas
    from {{ ref('stg_anp__precos') }} s
    join ultimo u on u.file_key = s._file_key
    where s.data_coleta is not null
    group by 1, 2
),
faixa as (
    select _file_key, generate_series(min(semana), max(semana), interval '1 week')::date as semana
    from coletas group by 1
)
select u.product_group, f._file_key, f.semana
from faixa f
join ultimo u on u.file_key = f._file_key
left join coletas c using (_file_key, semana)
where c.linhas is null
