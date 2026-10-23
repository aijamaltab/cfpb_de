-- 99a_drop_slot.sql -- УБОРКА, часть 1: слот. Запускает роль с правом REPLICATION (debezium) или суперпользователь.
-- ВАЖНО: сначала удалите коннекторы (bash cdc/scripts/connect.sh delete ИМЯ), иначе слот активен и удалить его нельзя.
select pg_drop_replication_slot(slot_name)
from pg_replication_slots
where slot_name = 'dbz_tabyldieva' and not active;

select slot_name, active from pg_replication_slots where slot_name = 'dbz_tabyldieva';
