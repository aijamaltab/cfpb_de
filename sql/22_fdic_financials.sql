-- Квартальные показатели банков США (FDIC /financials, открытый API без ключа). Размер банка, депозиты, прибыль по кварталам.
-- Запуск: psql -v stg=${PGUSER}_stg -v ods=${PGUSER}_ods -f sql/22_fdic_financials.sql
create table if not exists :"stg".stg_fdic_financials (
    cert    text,
    repdte  text,       -- дата отчёта YYYYMMDD (конец квартала)
    asset   text,       -- активы, тыс. USD
    dep     text,       -- депозиты, тыс. USD
    netinc  text,       -- чистая прибыль за период с начала года, тыс. USD
    roa     text,
    roe     text,
    eq      text,       -- собственный капитал, тыс. USD
    numemp  text,       -- число сотрудников
    _ds           date        not null,
    _source_file  text,
    _loaded_at    timestamptz not null default now()
);
create index if not exists ix_stg_fdic_fin_ds on :"stg".stg_fdic_financials (_ds);

create table if not exists :"ods".ods_fdic_financials (
    cert            bigint  not null,
    report_date     date    not null,
    assets_k_usd    bigint,
    deposits_k_usd  bigint,
    net_income_k_usd bigint,
    roa             numeric(10,4),
    roe             numeric(10,4),
    equity_k_usd    bigint,
    employees       int,
    ds              date    not null,
    primary key (cert, report_date)
);
create index if not exists ix_ods_fdic_fin_date on :"ods".ods_fdic_financials (report_date);

-- Откат: drop table if exists <login>_stg.stg_fdic_financials, <login>_ods.ods_fdic_financials;
--        delete from <login>_stg.etl_load_log where source like 'fdicfin_%';