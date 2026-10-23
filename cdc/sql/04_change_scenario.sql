-- 04_change_scenario.sql -- изменения в источнике для показа. Запускает владелец таблицы ПОСЛЕ создания коннекторов.
-- Запуск: psql -v ON_ERROR_STOP=1 -f cdc/sql/04_change_scenario.sql
-- Потом (через 5-30 секунд): python3 cdc/scripts/verify_cdc.py  и  select * from tabyldieva_ods.ods_complaint_cases where complaint_id = ...

-- 1. UPDATE: обращение закрыто. Ждём событие op = 'u': before (in_progress) и after (closed_relief).
update tabyldieva.complaint_cases
   set status = 'closed_relief'
 where complaint_id = (select complaint_id from tabyldieva.complaint_cases
                       where status <> 'closed_relief'
                       order by (status = 'in_progress') desc, complaint_id limit 1);

-- 2. INSERT: новое обращение. Ждём событие op = 'c'.
insert into tabyldieva.complaint_cases (complaint_id, product, company, state, status, timely, received_at)
values (999000001, 'Credit card', 'CDC DEMO COMPANY', 'CA', 'in_progress', null, now())
on conflict (complaint_id) do nothing;

-- 3. DELETE: обращение удалено (например, дубль). Ждём op = 'd' и следом пустое сообщение (tombstone).
delete from tabyldieva.complaint_cases where complaint_id = 999000001;

select status, count(*) as cases from tabyldieva.complaint_cases group by 1 order by 1;
