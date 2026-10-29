-- Как лежат данные по слоям: по экрану на слой, для скриншотов в презентацию. Только чтение.
--   psql -f sql/37_show_layers.sql            (весь файл подряд)
--   psql -v part=2 ...  не нужен: удобнее копировать нужный блок в psql и делать скрин после каждого
\pset border 2
\pset null '∅'
\pset footer off

\echo '=== 0. Все таблицы проекта по схемам: строки (оценка статистики Postgres) и размер ==='
select n.nspname as schema, c.relname as "table",
       to_char(s.n_live_tup, 'FM999 999 999') as rows,
       pg_size_pretty(pg_total_relation_size(c.oid)) as size
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
left join pg_stat_user_tables s on s.relid = c.oid
where c.relkind in ('r', 'p') and n.nspname in ('tabyldieva', 'tabyldieva_stg', 'tabyldieva_ods', 'tabyldieva_datamart',
                                               'tabyldieva_dbt', 'tabyldieva_dbt_snapshots')
  and (n.nspname <> 'tabyldieva' or c.relname = 'complaint_cases')     -- в схеме логина только источник CDC
order by array_position(array['tabyldieva_stg', 'tabyldieva_ods', 'tabyldieva_datamart', 'tabyldieva_dbt',
                              'tabyldieva_dbt_snapshots', 'tabyldieva'], n.nspname::text), c.relname;

\echo '=== 1. STG: сырые жалобы из API как есть (всё text) + служебные _ds, _source_file, _loaded_at ==='
select complaint_id, date_received, left(product, 22) product, left(company, 24) company,
       left(company_response, 24) company_response, timely, _ds, left(_source_file, 28) _source_file,
       _loaded_at::timestamp(0) _loaded_at
from tabyldieva_stg.stg_cfpb_complaints
where _ds = (select max(_ds) from tabyldieva_stg.stg_cfpb_complaints)
order by complaint_id limit 5;

\echo '=== 2. ODS: очищенные жалобы (типы, ключ complaint_id, ds) ==='
select complaint_id, received_at, left(product, 22) product, left(company, 24) company, state,
       left(company_response, 24) company_response, timely, ds
from tabyldieva_ods.ods_complaints
where ds = (select max(ds) from tabyldieva_ods.ods_complaints)
order by complaint_id limit 5;

\echo '=== 2a. ODS: справочники (компании, банк -> FDIC, ставка ФРС, финансы банков FDIC) ==='
select left(company_cfpb, 30) company_cfpb, company_type from tabyldieva_ods.ods_dim_company order by 1 limit 3;
select left(company_cfpb, 30) company_cfpb, cert from tabyldieva_ods.ods_company_bank order by 1 limit 3;
select * from tabyldieva_ods.ods_fred_dff order by 1 desc limit 3;
select cert, report_date, assets_k_usd, deposits_k_usd from tabyldieva_ods.ods_fdic_financials order by report_date desc, cert limit 3;

\echo '=== 3. DATAMART: витрины DAG (день x продукт, месяц x компания, квартал x банк) ==='
select * from tabyldieva_datamart.dm_complaints_daily
where ds = (select max(ds) from tabyldieva_datamart.dm_complaints_daily) order by complaints_cnt desc limit 5;
select month, left(company, 28) company, company_type, complaints_cnt, untimely_cnt, relief_cnt
from tabyldieva_datamart.dm_company_month
where month = (select max(month) from tabyldieva_datamart.dm_company_month) order by complaints_cnt desc limit 5;
select quarter_start, left(company, 28) company, banks_cnt, complaints_cnt, assets_k_usd, complaints_per_bn_assets
from tabyldieva_datamart.dm_bank_size_quarter
where quarter_start = (select max(quarter_start) from tabyldieva_datamart.dm_bank_size_quarter)
order by complaints_per_bn_assets desc limit 5;

\echo '=== 4. DBT: витрины dbt (штат x месяц, тип компании x день) ==='
select month, state, state_name, complaints_cnt, untimely_cnt, relief_cnt from tabyldieva_dbt.dm_state_month
where month = (select max(month) from tabyldieva_dbt.dm_state_month) order by complaints_cnt desc limit 5;
select ds, company_type, is_weekend, complaints_cnt, untimely_cnt, relief_cnt from tabyldieva_dbt.dm_type_daily
where ds = (select max(ds) from tabyldieva_dbt.dm_type_daily) order by complaints_cnt desc limit 5;

\echo '=== 5. CDC: источник -> Debezium/Kafka -> приёмник -> витрина обращений в работе ==='
select complaint_id, left(product, 22) product, status, received_at::timestamp(0), updated_at::timestamp(0)
from tabyldieva.complaint_cases order by updated_at desc limit 5;
select complaint_id, left(product, 22) product, status, received_at::timestamp(0), updated_at::timestamp(0)
from tabyldieva_ods.ods_complaint_cases order by updated_at desc limit 5;
select product, status, status_group, cases_cnt, avg_age_days from tabyldieva_dbt.dm_open_cases
where status_group = 'open' order by cases_cnt desc limit 5;

\echo '=== 6. Журнал загрузок etl_load_log: последние шаги ==='
select source, ds, status, rows_loaded, finished_at::timestamp(0) finished_at, left(error, 40) error
from tabyldieva_stg.etl_load_log order by id desc limit 12;

\echo '=== 7. Качество данных dq_metrics: последний день проверки ==='
select dimension, layer, table_name, metric, value, threshold, unit, status
from tabyldieva_datamart.dq_metrics
where check_date = (select max(check_date) from tabyldieva_datamart.dq_metrics)
order by array_position(array['полнота', 'уникальность', 'валидность', 'согласованность', 'точность', 'актуальность'], dimension),
         layer, metric;
