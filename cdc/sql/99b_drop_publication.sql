-- 99b_drop_publication.sql -- УБОРКА, часть 2: публикация. Запускает владелец таблицы. Порядок: сначала 99a.
drop publication if exists dbz_tabyldieva;
select pubname from pg_publication where pubname = 'dbz_tabyldieva';
