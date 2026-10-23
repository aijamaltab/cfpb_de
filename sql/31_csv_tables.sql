-- Таблицы CSV-источников: событие с датой (ставка ФРС, FRED DFF) и два справочника.
-- stg: всё text + _ds, _source_file, _loaded_at. ods: типы и первичный ключ.
-- Запуск: psql -v ON_ERROR_STOP=1 -v stg=${PGUSER}_stg -v ods=${PGUSER}_ods -f sql/31_csv_tables.sql
create schema if not exists :"stg";
create schema if not exists :"ods";

-- Событие с датой: ставка по федеральным фондам (эффективная), одна строка на календарный день.
create table if not exists :"stg".stg_fred_dff (
    observation_date text,
    dff              text,
    _ds           date        not null,
    _source_file  text,
    _loaded_at    timestamptz not null default now()
);
create index if not exists ix_stg_fred_dff_ds on :"stg".stg_fred_dff (_ds);

create table if not exists :"ods".ods_fred_dff (
    ds              date primary key,          -- день наблюдения
    fed_funds_rate  numeric(7,3),              -- процент годовых; NULL, если FRED отдал пропуск
    is_carried      boolean not null default false   -- true: за день ещё нет данных FRED, перенесена последняя известная ставка
);

-- Справочник компаний: тип (банк, финтех, бюро...) для витрин.
create table if not exists :"stg".stg_dim_company (
    company_cfpb    text,
    canonical_name  text,
    company_type    text,
    fdic_candidate  text,
    note            text,
    _ds           date        not null,
    _source_file  text,
    _loaded_at    timestamptz not null default now()
);

create table if not exists :"ods".ods_dim_company (
    company_cfpb    text    primary key,       -- название как в жалобах CFPB
    canonical_name  text    not null,
    company_type    text    not null,
    fdic_candidate  boolean,                   -- yes/no -> true/false; maybe -> NULL
    note            text
);

-- Справочник «компания в жалобах -> банк FDIC».
create table if not exists :"stg".stg_company_bank_map (
    company_cfpb  text,
    cert          text,
    bank_name     text,
    note          text,
    _ds           date        not null,
    _source_file  text,
    _loaded_at    timestamptz not null default now()
);

create table if not exists :"ods".ods_company_bank (
    company_cfpb  text   not null,
    cert          bigint not null,
    bank_name     text   not null,
    note          text,
    primary key (company_cfpb, cert)
);
