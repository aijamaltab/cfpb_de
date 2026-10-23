-- ClickHouse: копия витрины dm_complaints_daily (ФТ-7). Выполнять в ClickHouse, не в PostgreSQL.
-- База tabyldieva, имя таблицы совпадает с именем витрины. ReplacingMergeTree: повторная заливка дня не плодит дубли после merge.
-- Запуск (пароль вводится вручную, в файлах его нет):
--   read -s -p "CH password: " CHP; echo
--   curl -s --user admin:$CHP "http://clickhouse:8123/" --data-binary @sql/32_clickhouse_dm.sql
--   unset CHP
create table if not exists tabyldieva.dm_complaints_daily
(
    ds               Date,
    product          LowCardinality(String),
    complaints_cnt   UInt32,
    timely_cnt       UInt32,
    untimely_cnt     UInt32,
    in_progress_cnt  UInt32,
    relief_cnt       UInt32,
    fed_funds_rate   Nullable(Decimal(7, 3))
)
engine = ReplacingMergeTree
partition by toYYYYMM(ds)
order by (ds, product)
