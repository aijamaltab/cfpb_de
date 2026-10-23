-- Проверки контура бюро. Запуск: psql -v stg=${PGUSER}_stg -v ods=${PGUSER}_ods -v d1=2026-07-01 -v d2=2026-09-30 -f sql/13_bureau_checks.sql
\echo === 1. Покрытие: ждём 92 дня без дыр ===
select min(ds) first_day, max(ds) last_day, count(*) days, sum(n) rows
from (select ds, count(*) n from :"ods".ods_bureau group by ds) t;
\echo === 2. Дни-дыры: ждём 0 строк ===
select d::date missing_day from generate_series(:'d1'::date, :'d2'::date, '1 day') d
where d::date not in (select distinct ds from :"ods".ods_bureau);
\echo === 3. stg и ods: diff = 0 ===
select (select count(distinct complaint_id) from :"stg".stg_cfpb_bureau) stg_unique,
       (select count(*) from :"ods".ods_bureau) ods_rows;
\echo === 4. Продукты: ждём список бюро ===
select product, count(*) from :"ods".ods_bureau group by 1 order by 2 desc;
\echo === 5. Пересечение с основным контуром: ждём 0 ===
select count(*) from :"ods".ods_bureau b join :"ods".ods_complaints c using (complaint_id);
\echo === 6. Дни, где последний статус не success: ждём 0 строк ===
select ds, source, status, error from (
  select distinct on (source, ds) source, ds, status, error from :"stg".etl_load_log
  where source like 'bureau_%' order by source, ds, finished_at desc) x
where status <> 'success';
\echo === 7. Размер ===
select 'stg_cfpb_bureau' t, pg_size_pretty(pg_total_relation_size(:'stg' || '.stg_cfpb_bureau')) size
union all select 'ods_bureau', pg_size_pretty(pg_total_relation_size(:'ods' || '.ods_bureau'));
