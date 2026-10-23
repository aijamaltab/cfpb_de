-- Загрузка одного дня stg -> ods (delete + insert по ds). Повторный запуск даёт тот же результат.
-- Запуск: psql -v schema=${PGUSER}_stg -v ods=${PGUSER}_ods -v ds=2026-09-01 -f sql/06_ods_load_day.sql
begin;

delete from :"ods".ods_complaints where ds = :'ds'::date;

insert into :"ods".ods_complaints
    (complaint_id, received_at, sent_to_company_at, product, sub_product, issue, sub_issue, company,
     state, zip_code, company_response, company_public_response, timely, submitted_via, tags, ds)
select distinct on (complaint_id)
    complaint_id::bigint,
    (date_received::timestamptz at time zone 'UTC'),
    (nullif(date_sent_to_company, '')::timestamptz at time zone 'UTC'),
    product,
    nullif(sub_product, ''),
    nullif(issue, ''),
    nullif(sub_issue, ''),
    company,
    nullif(state, ''),
    nullif(zip_code, ''),
    nullif(company_response, ''),
    nullif(company_public_response, ''),
    case timely when 'Yes' then true when 'No' then false end,
    nullif(submitted_via, ''),
    nullif(tags, ''),
    _ds
from :"schema".stg_cfpb_complaints
where _ds = :'ds'::date
order by complaint_id, _loaded_at desc;

commit;
