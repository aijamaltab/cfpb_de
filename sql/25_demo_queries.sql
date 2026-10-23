-- Запросы для показа на защите (только читают). Порядок = порядок в docs/DEMO.md.
-- Цифры в заголовках верны на 17.10.2026 при окне до 30.09.2026; если окно расширят, они изменятся.
-- Запуск (из папки final_project):
--   psql -v stg=${PGUSER}_stg -v ods=${PGUSER}_ods -v dm=${PGUSER}_datamart -f sql/25_demo_queries.sql
\pset pager off

\echo
\echo '=== 1. Покрытие банковского контура: 2023-10-01 .. 2026-09-30, 1096 дней, 855 110 жалобы ==='
select min(ds) first_day, max(ds) last_day, count(distinct ds) days, count(*) complaints
from :"ods".ods_complaints;

\echo
\echo '=== 2. Слои сходятся: stg = ods = dm (банки) ==='
select (select count(distinct complaint_id) from :"stg".stg_cfpb_complaints) stg,
       (select count(*) from :"ods".ods_complaints) ods,
       (select sum(complaints_cnt) from :"dm".dm_complaints_daily) dm;

\echo
\echo '=== 3. Контур бюро: 92 дня, 1 909 762 жалобы; stg = ods ==='
select (select count(distinct ds) from :"ods".ods_bureau) days,
       (select count(distinct complaint_id) from :"stg".stg_cfpb_bureau) stg,
       (select count(*) from :"ods".ods_bureau) ods;

\echo
\echo '=== 4. Топ-10 компаний по жалобам за всё окно (банковские продукты) ==='
select company, sum(complaints_cnt) complaints,
       round(100.0 * sum(untimely_cnt) / sum(complaints_cnt), 2) pct_untimely,
       sum(relief_cnt) with_relief
from :"dm".dm_company_month
group by 1 order by 2 desc limit 10;

\echo
\echo '=== 5. Продукты: объём и доля просрочек ==='
select product, sum(complaints_cnt) complaints,
       round(100.0 * sum(untimely_cnt) / sum(complaints_cnt), 2) pct_untimely
from :"dm".dm_complaints_daily
group by 1 order by 2 desc;

\echo
\echo '=== 6. Журнал загрузок: статусы по источникам ==='
select source, status, count(*) runs, max(finished_at)::timestamp(0) last_finished
from :"stg".etl_load_log
group by 1, 2 order by 1, 2;

\echo
\echo '=== 7. Досинхронизация: последние запуски resync (days_fixed = сколько дней перезагружено) ==='
select source, ds as run_date, status, rows_loaded as days_fixed, left(coalesce(error, ''), 80) note
from :"stg".etl_load_log
where source like 'resync%'
order by started_at desc limit 6;

\echo
\echo '=== 8. Второй источник: FDIC, кварталы и банки ==='
select count(distinct report_date) quarters, min(report_date) first_q, max(report_date) last_q,
       count(*) rows, count(distinct cert) banks
from :"ods".ods_fdic_financials;

\echo
\echo '=== 9. Витрина: жалобы на 1 млрд USD активов, последний квартал (с оговоркой: грубая метрика) ==='
select company, banks_cnt, complaints_cnt, round(assets_k_usd / 1e6) assets_bn_usd, complaints_per_bn_assets
from :"dm".dm_bank_size_quarter
where quarter_start = (select max(quarter_start) from :"dm".dm_bank_size_quarter)
order by complaints_per_bn_assets desc limit 10;
