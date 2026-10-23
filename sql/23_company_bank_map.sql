-- Проверки справочника «компания в жалобах -> банк FDIC» (data/ref/company_bank_map.csv). Только читает.
-- Саму таблицу ods_company_bank наполняет DAG (задача load_ods_refs в final_tabyldieva_pipeline), здесь загрузки нет.
-- Запуск (из папки final_project): psql -v ods=${PGUSER}_ods -f sql/23_company_bank_map.sql

create temp table c_cnt as select company, count(*) complaints_cnt from :"ods".ods_complaints group by 1;

\echo === Проверка 1: компании из справочника, которых нет в жалобах (должно быть 0 строк) ===
select m.company_cfpb from :"ods".ods_company_bank m
left join c_cnt c on c.company = m.company_cfpb where c.company is null;

\echo === Проверка 2: банки из справочника, которых нет в показателях FDIC (должно быть 0 строк) ===
select m.cert, m.bank_name from :"ods".ods_company_bank m
where not exists (select 1 from :"ods".ods_fdic_financials f where f.cert = m.cert);

\echo === Итог: сколько жалоб покрывает справочник ===
select count(*) companies, (select count(*) from :"ods".ods_company_bank) banks,
       sum(c.complaints_cnt) complaints,
       round(100.0 * sum(c.complaints_cnt) / (select sum(complaints_cnt) from c_cnt), 1) pct_of_all
from (select distinct company_cfpb from :"ods".ods_company_bank) d
join c_cnt c on c.company = d.company_cfpb;
