-- Проставить новые колонки витрин для истории (ставка ФРС в витрине дня, тип компании в витрине месяца).
-- Нужно один раз после sql/30 и tools/backfill_fred.py: DAG заполняет эти колонки только для дат, которые он считает сам.
-- Повтор безопасен. Запуск (из папки final_project):
--   psql -v ON_ERROR_STOP=1 -v ods=${PGUSER}_ods -v dm=${PGUSER}_datamart -f sql/33_backfill_columns.sql

update :"dm".dm_complaints_daily d
   set fed_funds_rate = r.fed_funds_rate
  from :"ods".ods_fred_dff r
 where r.ds = d.ds and d.fed_funds_rate is distinct from r.fed_funds_rate;

update :"dm".dm_company_month m
   set company_type = coalesce((select c.company_type from :"ods".ods_dim_company c where c.company_cfpb = m.company), 'other')
 where m.company_type is distinct from coalesce((select c.company_type from :"ods".ods_dim_company c where c.company_cfpb = m.company), 'other');

\echo === Контроль: сколько строк витрин без ставки / без типа (ставки нет за дни вне файла FRED)
select (select count(*) from :"dm".dm_complaints_daily) as daily_rows,
       (select count(*) from :"dm".dm_complaints_daily where fed_funds_rate is null) as daily_without_rate,
       (select count(*) from :"dm".dm_company_month) as month_rows,
       (select count(*) from :"dm".dm_company_month where company_type is null) as month_without_type;
