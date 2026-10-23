-- Ручная сверка dbt и DAG без запуска dbt test. Запуск:  psql -f dbt/tabyldieva_dwh/variants/reconcile_dbt_vs_dag.sql
-- Ожидаем: расхождений 0 и одинаковые суммы.

-- 1. По дням: сколько строк в обеих витринах и в скольких числа разошлись.
select count(*)                                                  as строк_в_обеих,
       coalesce(sum((d.complaints_cnt <> g.complaints_cnt
                  or d.timely_cnt     <> g.timely_cnt
                  or d.untimely_cnt   <> g.untimely_cnt
                  or d.in_progress_cnt <> g.in_progress_cnt
                  or d.relief_cnt     <> g.relief_cnt)::int), 0) as расхождений
from tabyldieva_dbt.dm_complaints_daily d
join tabyldieva_datamart.dm_complaints_daily g using (ds, product);

-- 2. Строки, которые есть только с одной стороны.
select (select count(*) from tabyldieva_dbt.dm_complaints_daily)      as строк_dbt,
       (select count(*) from tabyldieva_datamart.dm_complaints_daily) as строк_dag;

-- 3. Одна и та же сумма в трёх разрезах dbt и в ods.
select (select sum(complaints_cnt) from tabyldieva_dbt.dm_complaints_daily) as по_дням,
       (select sum(complaints_cnt) from tabyldieva_dbt.dm_company_month)    as по_компаниям,
       (select sum(complaints_cnt) from tabyldieva_dbt.dm_state_month)      as по_штатам,
       (select count(*) from tabyldieva_ods.ods_complaints)                 as в_ods;
