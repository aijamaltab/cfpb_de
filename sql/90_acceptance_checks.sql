-- Самопроверка по критериям приёмки ФТТ. Только читает. Запуск (из папки final_project), дата = та, что гоняли в DAG:
--   psql -v ds=2026-09-14 -v stg=${PGUSER}_stg -v ods=${PGUSER}_ods -v dm=${PGUSER}_datamart -f sql/90_acceptance_checks.sql
-- НФТ-1: выполните до Clear и после, сравните числа в блоке «слои за дату».

\echo === ФТ-2: stg за дату (число строк = числу строк файла/ответа API за эту дату; служебные колонки заполнены)
select 'stg_cfpb_complaints' as t, count(*) as rows, count(_source_file) as with_source, count(_loaded_at) as with_loaded from :"stg".stg_cfpb_complaints where _ds = :'ds'
union all select 'stg_fred_dff', count(*), count(_source_file), count(_loaded_at) from :"stg".stg_fred_dff where _ds = :'ds'
union all select 'stg_dim_company (целиком)', count(*), count(_source_file), count(_loaded_at) from :"stg".stg_dim_company
union all select 'stg_company_bank_map (целиком)', count(*), count(_source_file), count(_loaded_at) from :"stg".stg_company_bank_map;

\echo === НФТ-1: слои за дату (до и после Clear числа должны совпасть)
select 'ods_complaints' as layer, count(*) as value from :"ods".ods_complaints where ds = :'ds'
union all select 'ods_fred_dff', count(*) from :"ods".ods_fred_dff where ds = :'ds'
union all select 'ods_dim_company', count(*) from :"ods".ods_dim_company
union all select 'ods_company_bank', count(*) from :"ods".ods_company_bank
union all select 'dm_complaints_daily: строк', count(*) from :"dm".dm_complaints_daily where ds = :'ds'
union all select 'dm_complaints_daily: сумма жалоб', coalesce(sum(complaints_cnt), 0) from :"dm".dm_complaints_daily where ds = :'ds'
union all select 'dm_company_month: сумма жалоб за месяц даты', coalesce(sum(complaints_cnt), 0) from :"dm".dm_company_month where month = date_trunc('month', :'ds'::date)::date
union all select 'dm_bank_size_quarter: строк', count(*) from :"dm".dm_bank_size_quarter
union all select 'etl_load_log: строк за дату', count(*) from :"stg".etl_load_log where ds = :'ds';

\echo === ФТ-3: типы и первичные ключи в ods
select table_name, column_name, data_type from information_schema.columns
where table_schema = :'ods' and ((table_name = 'ods_complaints' and column_name in ('complaint_id', 'received_at', 'timely', 'ds'))
   or (table_name = 'ods_fred_dff') or (table_name = 'ods_fdic_financials' and column_name in ('report_date', 'assets_k_usd')))
order by table_name, ordinal_position;
select conrelid::regclass as table_name, pg_get_constraintdef(oid) as primary_key from pg_constraint
where contype = 'p' and connamespace = :'ods'::regnamespace order by 1;

\echo === ФТ-4: последние 15 строк журнала
select id, source, ds, status, started_at, finished_at, rows_loaded, left(coalesce(error, ''), 60) as error
from :"stg".etl_load_log order by id desc limit 15;

\echo === ФТ-5: суммы метрики в двух витринах за один месяц совпадают (разность должна быть 0)
select (select coalesce(sum(complaints_cnt), 0) from :"dm".dm_complaints_daily
         where ds >= date_trunc('month', :'ds'::date) and ds < date_trunc('month', :'ds'::date) + interval '1 month') as daily_sum,
       (select coalesce(sum(complaints_cnt), 0) from :"dm".dm_company_month where month = date_trunc('month', :'ds'::date)::date) as month_sum;

\echo === Пример вопроса к витрине одним запросом без JOIN (ФТ-5): топ-5 компаний месяца по просрочкам
select company, company_type, complaints_cnt, untimely_cnt from :"dm".dm_company_month
where month = date_trunc('month', :'ds'::date)::date order by untimely_cnt desc, complaints_cnt desc limit 5;
