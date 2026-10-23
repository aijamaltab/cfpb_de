-- ODS: очищенный слой (как ods_orders у ментора): настоящие типы, понятные имена, ключ, ds без подчёркивания.
-- Запуск: psql -v ods=${PGUSER}_ods -f sql/05_ods_complaints.sql
create schema if not exists :"ods";

create table if not exists :"ods".ods_complaints (
    complaint_id             bigint primary key,
    received_at              timestamp   not null,   -- date_received из API, время по UTC
    sent_to_company_at       timestamp,              -- date_sent_to_company
    product                  text        not null,
    sub_product              text,
    issue                    text,
    sub_issue                text,
    company                  text        not null,
    state                    text,
    zip_code                 text,                   -- text: ведущие нули не терять
    company_response         text,                   -- статус, меняется со временем
    company_public_response  text,
    timely                   boolean,
    submitted_via            text,
    tags                     text,
    ds                       date        not null    -- день загрузки жалобы (= дата received_at по UTC)
);
create index if not exists ix_ods_complaints_ds on :"ods".ods_complaints (ds);
