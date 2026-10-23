-- Справочники ods (режим truncate + insert: пересобираются целиком из ods_complaints).
-- Запуск: psql -v ods=${PGUSER}_ods -f sql/07_ods_reference.sql
create schema if not exists :"ods";

create table if not exists :"ods".ods_products (
    product         text not null,
    sub_product     text not null default '',
    first_seen_ds   date not null,
    last_seen_ds    date not null,
    complaints_cnt  bigint not null,
    primary key (product, sub_product)
);

create table if not exists :"ods".ods_companies (
    company         text primary key,
    first_seen_ds   date not null,
    last_seen_ds    date not null,
    complaints_cnt  bigint not null
);
