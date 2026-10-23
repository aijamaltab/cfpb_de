-- 03_cdc_access.sql -- права для Debezium и ПУБЛИКАЦИЯ. Запускает ВЛАДЕЛЕЦ таблицы complaint_cases.
-- Нужно: владелец имеет право CREATE на базу, иначе create publication даст «permission denied for database».
-- Запуск: psql -v ON_ERROR_STOP=1 -v cdc_user=debezium -f cdc/sql/03_cdc_access.sql

select count(*) = 1 as role_exists from pg_roles where rolname = :'cdc_user' \gset
\if :role_exists
\else
    \echo ОШИБКА: роли :cdc_user нет. Сначала шаг 03a (его делает администратор).
    \quit
\endif

-- Снапшот коннектора = обычные SELECT-ы: нужны USAGE на схему и SELECT на таблицу.
select format('grant usage on schema tabyldieva to %I', :'cdc_user')
\gexec
select format('grant select on tabyldieva.complaint_cases to %I', :'cdc_user')
\gexec

-- Публикация: таблицы, изменения которых попадут в журнал для коннектора. Создаётся только если её ещё нет
-- (пересоздавать нельзя, пока её читает коннектор). truncate не публикуем: приёмник его не поймёт.
select 'create publication dbz_tabyldieva for table tabyldieva.complaint_cases '
       || 'with (publish = ''insert, update, delete'')'
where not exists (select 1 from pg_publication where pubname = 'dbz_tabyldieva')
\gexec

select pubname, pubowner::regrole as owner, pubinsert, pubupdate, pubdelete, pubtruncate
from pg_publication where pubname = 'dbz_tabyldieva';
select * from pg_publication_tables where pubname = 'dbz_tabyldieva' order by tablename;
