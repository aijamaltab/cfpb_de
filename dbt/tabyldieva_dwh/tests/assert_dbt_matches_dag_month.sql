-- Витрина «месяц x компания» на dbt равна витрине DAG (по числу жалоб, просрочкам и компенсациям).
-- Любая строка в ответе = расхождение: строка только с одной стороны или разные числа.
with dbt as (
    select month, company, complaints_cnt, untimely_cnt, relief_cnt
    from {{ ref('dm_company_month') }}
),
dag as (
    select month, company, complaints_cnt, untimely_cnt, relief_cnt
    from {{ source('datamart', 'dm_company_month') }}
)
select 'only_in_dbt' as side, * from (select * from dbt except select * from dag) a
union all
select 'only_in_dag' as side, * from (select * from dag except select * from dbt) b
