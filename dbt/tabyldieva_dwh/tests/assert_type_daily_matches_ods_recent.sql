-- Инкрементальная dm_type_daily не расходится с ods за последние lookback_days дней (число жалоб по дням).
-- Старше окна не сверяем: инкрементальная витрина их не пересчитывает, а дни, исправленные задним числом
-- (DAG cfpb_resync), лечит  dbt build --full-refresh -s dm_type_daily.
-- Тест проходит, когда запрос не вернул ни одной строки.
with win as (
    select max(ds) - {{ var('lookback_days') }} as ds_from from {{ ref('dm_type_daily') }}
),
t as (
    select ds, sum(complaints_cnt) as cnt from {{ ref('dm_type_daily') }}
    where ds >= (select ds_from from win) group by ds
),
o as (
    select ds, count(*) as cnt from {{ source('ods', 'ods_complaints') }}
    where ds >= (select ds_from from win) group by ds
)
select coalesce(t.ds, o.ds) as ds, t.cnt as type_daily_cnt, o.cnt as ods_cnt
from t full outer join o on o.ds = t.ds
where t.cnt is distinct from o.cnt
