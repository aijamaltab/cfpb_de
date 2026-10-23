-- stg: всё как пришло, все колонки text; три служебные колонки (_ds, _source_file, _loaded_at).
--   psql -v schema=${PGUSER}_stg -f sql/02_stg_cfpb.sql
create table if not exists :"schema".stg_cfpb_complaints (
    complaint_id             text,
    date_received            text,
    date_sent_to_company     text,
    product                  text,
    sub_product              text,
    issue                    text,
    sub_issue                text,
    company                  text,
    state                    text,
    zip_code                 text,
    company_response         text,
    company_public_response  text,
    timely                   text,
    submitted_via            text,
    tags                     text,
    _ds           date        not null,                 -- логический день запуска
    _source_file  text,                                 -- какой запрос (URL с параметрами)
    _loaded_at    timestamptz not null default now()    -- когда строка легла в таблицу
);
create index if not exists ix_stg_cfpb_complaints_ds on :"schema".stg_cfpb_complaints (_ds);
