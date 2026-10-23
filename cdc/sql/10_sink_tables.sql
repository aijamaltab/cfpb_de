-- 10_sink_tables.sql -- ИМИТАЦИЯ приёмника. Запускает tabyldieva. ЭТО НЕ CDC.
-- Нужен ТОЛЬКО если Kafka и Kafka Connect недоступны (чтобы dbt и проверки можно было собрать). Когда коннектор работает,
-- таблицу ods_complaint_cases создаёт и наполняет он сам (schema.evolution = basic), и этот файл запускать не надо.
-- На защите честно сказать, что настоящего Debezium в этом режиме нет.

create table if not exists tabyldieva_ods.ods_complaint_cases (
    complaint_id  bigint      primary key,
    product       text,
    company       text,
    state         text,
    status        text,
    timely        boolean,
    received_at   timestamptz,
    updated_at    timestamptz
);

insert into tabyldieva_ods.ods_complaint_cases
select complaint_id, product, company, state, status, timely, received_at, updated_at from tabyldieva.complaint_cases
on conflict (complaint_id) do update set
    product = excluded.product, company = excluded.company, state = excluded.state, status = excluded.status,
    timely = excluded.timely, received_at = excluded.received_at, updated_at = excluded.updated_at;

delete from tabyldieva_ods.ods_complaint_cases c
where not exists (select 1 from tabyldieva.complaint_cases s where s.complaint_id = c.complaint_id);

select count(*) as rows from tabyldieva_ods.ods_complaint_cases;
