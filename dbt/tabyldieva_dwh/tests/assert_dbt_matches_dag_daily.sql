-- ФТ-8: результат dbt-модели равен витрине, которую считает DAG. Любая строка в ответе = расхождение.
-- fed_funds_rate не сверяем: файл ставки обновляется отдельно, и значение за день может появиться позже.
-- Тест проходит, когда запрос не вернул ни одной строки.
with dbt as (
    select ds, product, complaints_cnt, timely_cnt, untimely_cnt, in_progress_cnt, relief_cnt
    from {{ ref('dm_complaints_daily') }}
),
dag as (
    select ds, product, complaints_cnt, timely_cnt, untimely_cnt, in_progress_cnt, relief_cnt
    from {{ source('datamart', 'dm_complaints_daily') }}
)
select 'only_in_dbt' as side, * from (select * from dbt except select * from dag) a
union all
select 'only_in_dag' as side, * from (select * from dag except select * from dbt) b
