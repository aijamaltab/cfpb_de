-- 05_slot_monitor.sql -- не копит ли слот журнал на диске. Запускает владелец.
-- Слот держит WAL, пока читатель его не получил. active = t значит, что коннектор подключён.
select slot_name,
       active,
       wal_status,                                        -- reserved/extended: ок; unreserved/lost: беда
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))          as wal_kept_for_slot,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn))  as unread_by_connector
from pg_replication_slots
order by slot_name;
