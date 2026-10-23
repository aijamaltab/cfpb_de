-- Итоговые проверки загруженного окна. Все запросы только читают.
-- Запуск: psql -v stg=${PGUSER}_stg -v ods=${PGUSER}_ods -v dm=${PGUSER}_datamart -v d1=2023-10-01 -v d2=2026-09-30 -f sql/11_window_checks.sql

\echo '=== 1. Покрытие: ждём 2023-10-01 .. 2026-09-30, 1096 дней ==='
select min(ds) as first_day, max(ds) as last_day, count(distinct ds) as days, count(*) as rows from :"ods".ods_complaints;

\echo '=== 2. Дни-дыры в окне: ждём 0 строк ==='
select d::date as missing_day
from generate_series(:'d1'::date, :'d2'::date, '1 day') d
left join (select distinct ds from :"ods".ods_complaints) o on o.ds = d::date
where o.ds is null;

\echo '=== 3. stg и ods: уникальных жалоб в stg = строк в ods (diff = 0) ==='
select (select count(distinct complaint_id) from :"stg".stg_cfpb_complaints) as stg_unique,
       (select count(*) from :"ods".ods_complaints) as ods_rows,
       (select count(distinct complaint_id) from :"stg".stg_cfpb_complaints) - (select count(*) from :"ods".ods_complaints) as diff;

\echo '=== 4. Витрина dm_complaints_daily против ods: diff = 0 ==='
select (select sum(complaints_cnt) from :"dm".dm_complaints_daily) as dm_rows,
       (select count(*) from :"ods".ods_complaints) as ods_rows,
       (select sum(complaints_cnt) from :"dm".dm_complaints_daily) - (select count(*) from :"ods".ods_complaints) as diff;

\echo '=== 5. Журнал: падения по источникам (в идеале ни одного failed, либо повторы закрыты success) ==='
select source, status, count(*) from :"stg".etl_load_log
where source in ('stg_cfpb_complaints','ods_complaints','ods_products','ods_companies','dm_complaints_daily','dm_company_month','dq_check','clickhouse_complaints')
group by 1, 2 order by 1, 2;

\echo '=== 6. Дни, где последний статус в журнале не success: ждём 0 строк ==='
select ds, source, status, left(error, 120) as error from (
    select distinct on (source, ds) source, ds, status, error from :"stg".etl_load_log
    where source in ('stg_cfpb_complaints','ods_complaints','dq_check') order by source, ds, id desc) t
where status <> 'success' order by ds;

\echo '=== 7. Продукты в ods: ждём ровно 8 ==='
select product, count(*) from :"ods".ods_complaints group by 1 order by 2 desc;

\echo '=== 8. Объём по годам (глазами: рост без обрывов) ==='
select date_trunc('year', ds)::date as year_start, count(*) as rows, count(distinct ds) as days from :"ods".ods_complaints group by 1 order by 1;

\echo '=== 9. Самые маленькие дни (выходные ~200-300 это нормально; ноль или единицы подозрительно) ==='
select ds, count(*) as n from :"ods".ods_complaints group by 1 order by 2 limit 10;

\echo '=== 10. Пустые значения в ключевых полях: ждём нули ==='
select count(*) filter (where coalesce(company,'')='') as no_company, count(*) filter (where coalesce(product,'')='') as no_product,
       count(*) filter (where received_at is null) as no_date from :"ods".ods_complaints;

\echo '=== 11. Размер таблиц ==='
select schemaname || '.' || relname as table_name, pg_size_pretty(pg_total_relation_size(relid)) as size
from pg_stat_user_tables where schemaname in (:'stg', :'ods', :'dm') order by pg_total_relation_size(relid) desc;
