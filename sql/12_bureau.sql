-- Отдельный контур: кредитные бюро и коллекторы (июль-сентябрь 2026). Структура как у основного контура.
-- Запуск: psql -v stg=${PGUSER}_stg -v ods=${PGUSER}_ods -f sql/12_bureau.sql
create table if not exists :"stg".stg_cfpb_bureau (like :"stg".stg_cfpb_complaints including all);
create table if not exists :"ods".ods_bureau        (like :"ods".ods_complaints including all);

-- Откат этого контура (только если нужно начать заново):
--   drop table if exists <login>_stg.stg_cfpb_bureau, <login>_ods.ods_bureau;
--   delete from <login>_stg.etl_load_log where source like '%bureau%';
