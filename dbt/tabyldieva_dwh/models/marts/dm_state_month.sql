-- Витрина «месяц x штат»: где больше жалоб. Названия штатов -- из seed us_states.
-- Штат, которого нет в справочнике (или пустой), попадает в строку 'unknown': жалоба не теряется.
select
    e.month_start                                          as month,
    e.state,
    e.state_name,
    count(*)::int                                          as complaints_cnt,
    (count(*) filter (where e.timely is false))::int       as untimely_cnt,
    (count(*) filter (where e.is_relief))::int             as relief_cnt,
    '{{ invocation_id }}'                                  as _dbt_run_id,
    now()                                                  as _built_at
from {{ ref('int_complaints_enriched') }} e
group by e.month_start, e.state, e.state_name
