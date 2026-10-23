-- ClickHouse: копии остальных витрин (рядом с dm_complaints_daily из sql/32). Выполнять в ClickHouse, не в PostgreSQL.
-- База tabyldieva, имя таблицы совпадает с именем витрины, колонки те же (служебные _dbt_run_id и _built_at не копируются).
-- Шесть таблиц: три ведёт DAG витрин, две dbt-витрины скрипт или DAG dbt, одну (dm_open_cases) DAG CDC.
-- ReplacingMergeTree: повторная заливка не плодит дубли после merge; задачи DAG и скрипт сначала удаляют срез, потом вставляют.
-- ClickHouse по HTTP принимает один запрос за раз, поэтому файл целиком curl-ом не отправляется. Создание таблиц:
--   read -s -p "CH password: " CH_PASSWORD; echo; export CH_PASSWORD
--   CH_HOST=clickhouse CH_PORT=8123 CH_USER=admin CH_DB=tabyldieva python3 tools/clickhouse_backfill_marts.py --create
-- (скрипт режет файл по `;` в конце строки и выполняет запросы по одному; повтор безопасен: if not exists)

-- DAG final_tabyldieva_datamart: месяц x компания (задача load_clickhouse_company_month, срез: месяц дня ds)
create table if not exists tabyldieva.dm_company_month
(
    month           Date,
    company         String,
    company_type    LowCardinality(Nullable(String)),
    complaints_cnt  UInt32,
    untimely_cnt    UInt32,
    relief_cnt      UInt32
)
engine = ReplacingMergeTree
partition by toYear(month)
order by (month, company);

-- DAG final_tabyldieva_datamart: квартал x компания (задача load_clickhouse_bank_size, пересборка целиком, как в PostgreSQL)
create table if not exists tabyldieva.dm_bank_size_quarter
(
    quarter_start               Date,
    company                     String,
    banks_cnt                   UInt16,
    complaints_cnt              UInt32,
    assets_k_usd                Int64,
    deposits_k_usd              Nullable(Int64),
    complaints_per_bn_assets    Decimal(12, 4),
    complaints_per_bn_deposits  Nullable(Decimal(12, 4))
)
engine = ReplacingMergeTree
partition by toYear(quarter_start)
order by (quarter_start, company);

-- dbt-витрина (схема tabyldieva_dbt): месяц x штат. Копируется скриптом после `dbt build` (DAG её не строит)
create table if not exists tabyldieva.dm_state_month
(
    month           Date,
    state           String,
    state_name      Nullable(String),
    complaints_cnt  UInt32,
    untimely_cnt    UInt32,
    relief_cnt      UInt32
)
engine = ReplacingMergeTree
partition by toYear(month)
order by (month, state);

-- dbt-витрина (схема tabyldieva_dbt): день x тип компании. Копируется скриптом после `dbt build`
create table if not exists tabyldieva.dm_type_daily
(
    ds              Date,
    company_type    LowCardinality(String),
    is_weekend      Bool,
    complaints_cnt  UInt32,
    untimely_cnt    UInt32,
    relief_cnt      UInt32,
    fed_funds_rate  Nullable(Decimal(7, 3))
)
engine = ReplacingMergeTree
partition by toYYYYMM(ds)
order by (ds, company_type);

-- dbt-витрина слоя CDC (схема tabyldieva_dbt): продукт x статус, обращения «в работе» и возраст. Снимок «на сейчас»:
-- копируется целиком каждый час задачей load_clickhouse_open_cases DAG final_tabyldieva_cdc_integration
create table if not exists tabyldieva.dm_open_cases
(
    product         LowCardinality(String),
    status          LowCardinality(String),
    status_group    LowCardinality(Nullable(String)),
    cases_cnt       UInt32,
    avg_age_days    Nullable(Decimal(10, 1))
)
engine = ReplacingMergeTree
order by (product, status);
