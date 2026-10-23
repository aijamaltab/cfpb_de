{{ config(tags=['cdc']) }}
-- Витрина dm_open_cases не теряет обращения: сумма по ней равна числу строк в приёмнике CDC.
select m.total, s.cnt
from (select coalesce(sum(cases_cnt), 0) as total from {{ ref('dm_open_cases') }}) m
cross join (select count(*) as cnt from {{ source('cdc', 'ods_complaint_cases') }}) s
where m.total <> s.cnt
