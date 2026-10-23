-- ClickHouse: таблица-витрина жалоб (выполнять в ClickHouse, не в PostgreSQL).
-- ReplacingMergeTree по complaint_id: повторная загрузка дня не плодит дубли после merge.
-- База tabyldieva (подключение: host clickhouse, порт 8123 HTTP). Создание: см. README, раздел ClickHouse.
create table if not exists tabyldieva.cfpb_complaints
(
    complaint_id     UInt64,
    received_at      DateTime,
    ds               Date,
    product          LowCardinality(String),
    sub_product      String,
    issue            String,
    company          String,
    state            LowCardinality(String),
    company_response LowCardinality(String),
    timely           UInt8,
    submitted_via    LowCardinality(String)
)
engine = ReplacingMergeTree
partition by toYYYYMM(ds)
order by (ds, product, complaint_id);
