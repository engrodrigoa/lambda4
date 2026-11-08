-- Calendário diário de 2004 ao fim do ano corrente (cobre toda a série da ANP).
with dias as (
    select generate_series(date '2004-01-01',
                           (date_trunc('year', current_date) + interval '1 year - 1 day')::date,
                           interval '1 day')::date as data
)
select
    data,
    extract(year from data)::int                         as ano,
    extract(quarter from data)::int                      as trimestre,
    case when extract(month from data) <= 6 then 1 else 2 end as semestre,
    extract(month from data)::int                        as mes,
    date_trunc('month', data)::date                      as mes_inicio,
    to_char(data, 'YYYY-MM')                             as ano_mes,
    date_trunc('week', data)::date                       as semana_inicio,      -- segunda-feira (ISO)
    to_char(data, 'IYYY-"S"IW')                          as ano_semana_iso,
    extract(isodow from data)::int                       as dia_semana_iso,     -- 1 = segunda
    (array['Segunda','Terça','Quarta','Quinta','Sexta','Sábado','Domingo'])[extract(isodow from data)::int]
                                                         as nome_dia_semana,
    extract(isodow from data) in (6, 7)                  as fl_fim_de_semana
from dias
