-- 00_preflight.sql -- РАЗВЕДКА: годится ли база в источники CDC. Только читает. Запускает tabyldieva.
-- Запуск: psql -f cdc/sql/00_preflight.sql

-- 1. Нужно значение logical. Без него Debezium не заработает (меняет только администратор сервера).
show wal_level;

-- 2. Мои права: rolreplication нужен РОЛИ КОННЕКТОРА (debezium), а не мне.
--    can_create_in_db = t нужно ВЛАДЕЛЬЦУ таблиц: без права CREATE на базу публикацию не создать.
select rolname, rolreplication, rolsuper,
       has_database_privilege(rolname, current_database(), 'create') as can_create_in_db
from pg_roles where rolname = current_user;

-- 3. Роль коннектора существует и умеет читать журнал? (Если строк нет: шаг 03a, его делает администратор.)
select rolname, rolreplication from pg_roles where rolname = 'debezium';

-- 4. Схема-источник и её владелец (если схемы ещё нет, строк не будет: это шаг 01).
select nspname, pg_get_userbyid(nspowner) as owner from pg_namespace where nspname = current_user;

-- 5. Забытые слоты. Чужой слот копит журнал на диске.
select slot_name, active, wal_status from pg_replication_slots;
