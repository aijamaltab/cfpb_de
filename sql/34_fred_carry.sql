-- 34_fred_carry.sql -- колонка is_carried в ods_fred_dff: ставка за самые свежие дни переносится с последнего известного дня.
-- Нужно один раз на существующей базе (на новой её уже создаёт sql/31). Повтор безопасен.
-- Зачем: ряд FRED выходит с задержкой 1-3 дня, а DAG работает по расписанию и за выбранную преподавателем дату.
-- Запуск: psql -v ON_ERROR_STOP=1 -v ods=${PGUSER}_ods -f sql/34_fred_carry.sql
alter table :"ods".ods_fred_dff add column if not exists is_carried boolean not null default false;

select count(*) filter (where is_carried) as carried_rows, count(*) as all_rows, max(ds) as last_ds from :"ods".ods_fred_dff;
