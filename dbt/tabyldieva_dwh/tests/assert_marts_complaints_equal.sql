-- Главный инвариант витрин: сколько жалоб в ods, столько и в каждой витрине. Разные разрезы (день x продукт,
-- месяц x компания, месяц x штат) дают одну и ту же сумму. Если разошлись, при соединении со справочником
-- потерялись или задвоились жалобы.
-- Тест проходит, когда запрос не вернул ни одной строки.
with ods as (
    select count(*) as cnt from {{ source('ods', 'ods_complaints') }}
),
marts as (
    select
        (select sum(complaints_cnt) from {{ ref('dm_complaints_daily') }}) as daily,
        (select sum(complaints_cnt) from {{ ref('dm_company_month') }})    as company_month,
        (select sum(complaints_cnt) from {{ ref('dm_state_month') }})      as state_month
)
select ods.cnt, marts.daily, marts.company_month, marts.state_month
from ods cross join marts
where marts.daily         is distinct from ods.cnt
   or marts.company_month is distinct from ods.cnt
   or marts.state_month   is distinct from ods.cnt
