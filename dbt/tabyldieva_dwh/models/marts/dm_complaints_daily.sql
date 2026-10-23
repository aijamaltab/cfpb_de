-- Та же витрина, что считает DAG final_tabyldieva_datamart (зерно: день x продукт), но на dbt.
-- Пересобирается целиком (table), а не инкрементально: CFPB публикует жалобы с опозданием, и старые дни
-- меняются задним числом (их исправляет DAG cfpb_resync). Инкрементальный вариант -- в variants/.
-- Результат сверяет тест tests/assert_dbt_matches_dag_daily.sql.
select
    e.ds,
    e.product,
    count(*)::int                                          as complaints_cnt,
    (count(*) filter (where e.timely))::int                as timely_cnt,
    (count(*) filter (where e.timely is false))::int       as untimely_cnt,
    (count(*) filter (where e.is_in_progress))::int        as in_progress_cnt,
    (count(*) filter (where e.is_relief))::int             as relief_cnt,
    r.fed_funds_rate,
    '{{ invocation_id }}'                                  as _dbt_run_id,
    now()                                                  as _built_at
from {{ ref('int_complaints_enriched') }} e
left join {{ source('ods', 'ods_fred_dff') }} r on r.ds = e.ds
group by e.ds, e.product, r.fed_funds_rate
