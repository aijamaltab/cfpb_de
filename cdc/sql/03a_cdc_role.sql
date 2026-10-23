-- 03a_cdc_role.sql -- роль для Debezium. Запускает суперпользователь. ЕСЛИ роль debezium на стенде уже есть, шаг не нужен.
-- Проверка: select rolname, rolreplication from pg_roles where rolname = 'debezium';
-- Запуск: psql ... -v cdc_user=debezium -v cdc_pass=ПАРОЛЬ -f cdc/sql/03a_cdc_role.sql

select format('create role %I login replication password %L', :'cdc_user', :'cdc_pass')
where not exists (select 1 from pg_roles where rolname = :'cdc_user')
\gexec

select format('alter role %I with login replication', :'cdc_user')
\gexec

select rolname, rolreplication, rolsuper from pg_roles where rolname = :'cdc_user';
