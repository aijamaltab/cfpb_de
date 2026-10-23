-- Первая версия витрины dm_type_daily: обычная таблица (table), каждый запуск пересчитывает всё. В сборку не входит (папка variants).
-- Чтобы вернуться к ней: положить файл в models/marts/ вместо dm_type_daily.sql.
-- Плюс: всегда точная, дни, исправленные задним числом, подхватываются сами. Минус: время растёт вместе с данными
-- (у нас 0,86 млн жалоб, это секунды; на сотнях миллионов строк понадобится инкрементальная версия из основного проекта).
select
    e.ds,
    e.company_type,
    d.is_weekend,
    count(*)::int                                          as complaints_cnt,
    (count(*) filter (where e.timely is false))::int       as untimely_cnt,
    (count(*) filter (where e.is_relief))::int             as relief_cnt,
    r.fed_funds_rate,
    '{{ invocation_id }}'                                  as _dbt_run_id,
    now()                                                  as _built_at
from {{ ref('int_complaints_enriched') }} e
join {{ ref('dim_calendar') }} d on d.ds = e.ds
left join {{ source('ods', 'ods_fred_dff') }} r on r.ds = e.ds
group by e.ds, e.company_type, d.is_weekend, r.fed_funds_rate
