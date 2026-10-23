-- Витрины (dm): готовые таблицы для дашборда. Режим: delete + insert за день / за месяц, повтор безопасен.
-- Запуск: psql -v dm=${PGUSER}_datamart -f sql/09_dm.sql
create schema if not exists :"dm";

-- жалобы по дням и продуктам: сколько, в срок, в работе, с компенсацией
create table if not exists :"dm".dm_complaints_daily (
    ds               date   not null,
    product          text   not null,
    complaints_cnt   int    not null,
    timely_cnt       int    not null,
    untimely_cnt     int    not null,
    in_progress_cnt  int    not null,
    relief_cnt       int    not null,      -- закрыто с денежной или неденежной компенсацией
    fed_funds_rate   numeric(7,3),         -- ставка ФРС на этот день (CSV-источник FRED DFF), % годовых
    primary key (ds, product)
);

-- рейтинг компаний по месяцам: объём жалоб, просрочки, компенсации
create table if not exists :"dm".dm_company_month (
    month            date   not null,      -- первое число месяца
    company          text   not null,
    company_type     text,                 -- тип компании из справочника dim_company (other, если нет в справочнике)
    complaints_cnt   int    not null,
    untimely_cnt     int    not null,
    relief_cnt       int    not null,
    primary key (month, company)
);
