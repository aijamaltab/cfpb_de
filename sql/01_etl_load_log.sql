-- Журнал загрузок
-- Запуск: psql -v schema=tabyldieva_stg -f sql/01_etl_load_log.sql
create schema if not exists :"schema";

create table if not exists :"schema".etl_load_log (
    id           bigserial primary key,
    source       text        not null,
    ds           date        not null,
    status       text        not null,          -- running / success / failed
    started_at   timestamptz not null default now(),
    finished_at  timestamptz,
    rows_loaded  integer,
    error        text
);
