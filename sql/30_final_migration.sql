-- Миграция под ФТТ: схема витрин -> <логин>_datamart, новые колонки витрин, схема <логин>_meta для проверки ментора.
-- Безопасно запускать повторно. Данные не удаляются: схема переименовывается, колонки добавляются.
-- Запускать ПОСЛЕ sql/01_etl_load_log.sql и ДО sql/09_dm.sql (иначе новая пустая схема помешает переименованию).
-- Запуск (из папки final_project; пароль вводится через read -s PGPASSWORD; export PGPASSWORD):
--   psql -v ON_ERROR_STOP=1 -v stg=${PGUSER}_stg -v ods=${PGUSER}_ods -v dm=${PGUSER}_datamart \
--        -v dm_old=${PGUSER}_dm -v meta=${PGUSER}_meta -f sql/30_final_migration.sql
-- Если на переименовании ошибка permission denied: пересоберите витрины в новой схеме (sql/09_dm.sql + DAG витрин).

-- 1. Переименовать схему витрин (только если старая есть, а новой ещё нет).
select format('alter schema %I rename to %I', :'dm_old', :'dm')
where exists (select 1 from pg_namespace where nspname = :'dm_old')
  and not exists (select 1 from pg_namespace where nspname = :'dm')
\gexec

-- 2. Схемы (на чистом стенде создаются здесь).
create schema if not exists :"dm";
create schema if not exists :"meta";

-- 3. Новые колонки витрин. На чистом стенде таблиц ещё нет: их создаст sql/09_dm.sql, тогда этот блок пропускается.
select format('alter table %I.dm_complaints_daily add column if not exists fed_funds_rate numeric(7,3)', :'dm')
where to_regclass(format('%I.dm_complaints_daily', :'dm')) is not null
\gexec
select format('alter table %I.dm_company_month add column if not exists company_type text', :'dm')
where to_regclass(format('%I.dm_company_month', :'dm')) is not null
\gexec

-- 4. Совместимость с tools/check_project.py ментора: он ищет <логин>_meta.load_log.
--    Настоящий журнал по ФТТ остаётся <логин>_stg.etl_load_log; здесь только его отражение.
create or replace view :"meta".load_log as
select id as load_id, source, ds, status, started_at, finished_at, rows_loaded, error
from :"stg".etl_load_log;
comment on view :"meta".load_log is 'Отражение <логин>_stg.etl_load_log для проверки ментора. Журнал по ФТТ: etl_load_log.';

select nspname as schema from pg_namespace where nspname in (:'stg', :'ods', :'dm', :'dm_old', :'meta') order by 1;
