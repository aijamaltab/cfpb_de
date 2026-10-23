-- 01_source_tables.sql -- «БАЗА САЙТА»: схема-источник tabyldieva и таблица complaint_cases (имитация системы обработки обращений).
-- Запускает владелец схемы (tabyldieva; если нет права: администратор, и тогда схемой владеет он).
-- Запуск: psql -v ON_ERROR_STOP=1 -f cdc/sql/01_source_tables.sql
--
-- Зачем имитация: у публичного API CFPB нет потока изменений. Мы создаём внутреннюю таблицу обращений, статусы которой
-- меняются, и именно её изменения Debezium несёт в хранилище. Честно сказать комиссии: изменения синтетические.

create schema if not exists tabyldieva;

create table if not exists tabyldieva.complaint_cases (
    complaint_id  bigint      primary key,          -- номер жалобы CFPB; станет ключом события
    product       text        not null,
    company       text        not null,
    state         text,
    status        text        not null
                  check (status in ('in_progress', 'closed_explanation', 'closed_relief', 'closed_other')),
    timely        boolean,
    received_at   timestamptz not null,
    updated_at    timestamptz not null default now()
);

-- updated_at двигается при любом UPDATE (по нему видно свежесть приёмника).
create or replace function tabyldieva.set_updated_at()
returns trigger language plpgsql as $$
begin
    new.updated_at := now();
    return new;
end;
$$;

drop trigger if exists trg_complaint_cases_updated_at on tabyldieva.complaint_cases;
create trigger trg_complaint_cases_updated_at
    before update on tabyldieva.complaint_cases
    for each row execute function tabyldieva.set_updated_at();

-- REPLICA IDENTITY FULL: в событии смены статуса будет и старое значение (before), и новое (after).
alter table tabyldieva.complaint_cases replica identity full;

select schemaname, tablename, tableowner from pg_tables where schemaname = 'tabyldieva';
