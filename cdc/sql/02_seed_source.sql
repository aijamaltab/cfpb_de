-- 02_seed_source.sql -- заливаем в источник обращения за последние 30 дней из ods_complaints. Запускает владелец таблицы.
-- Заливаем ДО коннектора: Debezium найдёт эти строки при снапшоте (op = r).
-- Запуск: psql -v ON_ERROR_STOP=1 -f cdc/sql/02_seed_source.sql
-- Статус: из company_response CFPB. 'In progress' -> in_progress; закрытия -> closed_*; остальное -> closed_other.

insert into tabyldieva.complaint_cases (complaint_id, product, company, state, status, timely, received_at)
select complaint_id, product, company, state,
       case company_response
            when 'In progress'                  then 'in_progress'
            when 'Closed with explanation'      then 'closed_explanation'
            when 'Closed with monetary relief'  then 'closed_relief'
            when 'Closed with non-monetary relief' then 'closed_relief'
            else 'closed_other'
       end,
       timely,
       received_at at time zone 'UTC'
from tabyldieva_ods.ods_complaints
where ds > (select max(ds) from tabyldieva_ods.ods_complaints) - 30
on conflict (complaint_id) do nothing;

select status, count(*) as cases from tabyldieva.complaint_cases group by 1 order by 1;
