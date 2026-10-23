-- Проверки после загрузки. Первые четыре должны вернуть 0 строк.
--   psql -v schema=${PGUSER}_stg -v d1=2026-09-01 -v d2=2026-09-14 -f sql/04_dq_checks.sql

-- 1. Пустые дни окна (тревога, а не успех)
select d::date as empty_day
from generate_series(:'d1'::date, :'d2'::date, '1 day') d
left join (select distinct _ds from :"schema".stg_cfpb_complaints) s on s._ds = d::date
where s._ds is null;

-- 2. Дубли ключа внутри одного дня
select _ds, complaint_id, count(*) from :"schema".stg_cfpb_complaints
group by 1,2 having count(*) > 1;

-- 3. «Просили» и «ответил API»: дата жалобы не равна дню запуска
select _ds, count(*) from :"schema".stg_cfpb_complaints
where left(date_received, 10) <> _ds::text group by 1;

-- 4. Жалоба попала в разные дни (одна и та же complaint_id в двух _ds)
select complaint_id, count(distinct _ds) from :"schema".stg_cfpb_complaints
group by 1 having count(distinct _ds) > 1 limit 20;

-- 5. Объём по дням против среднего (глазами; если день < 50% среднего, смотреть источник)
select _ds, count(*) n, round(avg(count(*)) over (), 0) avg_n
from :"schema".stg_cfpb_complaints group by 1 order by 1;

-- 6. Доля пустых в важных колонках
select round(100.0*count(*) filter (where nullif(company,'') is null)/count(*),2) pct_null_company,
       round(100.0*count(*) filter (where nullif(product,'') is null)/count(*),2) pct_null_product,
       round(100.0*count(*) filter (where nullif(company_response,'') is null)/count(*),2) pct_null_response
from :"schema".stg_cfpb_complaints;

-- 7. Свежесть: последняя успешная загрузка
select max(finished_at) last_success from :"schema".etl_load_log where status='success';
