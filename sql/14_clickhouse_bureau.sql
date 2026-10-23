-- ClickHouse: таблица контура бюро (выполнять в ClickHouse). Структура как у cfpb_complaints.
create table if not exists tabyldieva.cfpb_bureau
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
