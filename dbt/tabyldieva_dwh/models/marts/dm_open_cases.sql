{{ config(tags=['cdc']) }}

-- Бизнес-вопрос 4: сколько обращений «в работе» и как давно они открыты.
-- Источник: CDC (текущее состояние обращений), статус -- через справочник-seed complaint_status.
select
    c.product,
    c.status,
    s.status_group,
    count(*)::int                                                              as cases_cnt,
    round(avg(extract(epoch from (now() - c.received_at)) / 86400)::numeric, 1) as avg_age_days,
    '{{ invocation_id }}'                                                      as _dbt_run_id,
    now()                                                                      as _built_at
from {{ source('cdc', 'ods_complaint_cases') }} c
left join {{ ref('complaint_status') }} s on s.status = c.status
group by c.product, c.status, s.status_group
