-- Витрина «месяц x компания» (зерно как в DAG); тип компании из справочника-CSV.
select
    e.month_start                                          as month,
    e.company,
    e.company_type,
    count(*)::int                                          as complaints_cnt,
    (count(*) filter (where e.timely is false))::int       as untimely_cnt,
    (count(*) filter (where e.is_relief))::int             as relief_cnt,
    '{{ invocation_id }}'                                  as _dbt_run_id,
    now()                                                  as _built_at
from {{ ref('int_complaints_enriched') }} e
group by e.month_start, e.company, e.company_type
