# CDC: изменения статусов обращений (Debezium + Kafka)

**Таблица:** `tabyldieva.complaint_cases` («база сайта»). Изменения этой таблицы через Debezium и Kafka попадают в `tabyldieva_ods.ods_complaint_cases` (текущее состояние: одна строка на обращение).

**Честно про данные.** У публичного API CFPB нет потока изменений, поэтому `complaint_cases` — имитация внутренней системы обработки обращений банка. Стартовые строки взяты из реальных жалоб (последние 30 дней из `ods_complaints`), а статусы меняет скрипт `sql/04_change_scenario.sql`. Механика CDC настоящая (журнал WAL → Debezium → Kafka → sink), сами изменения синтетические.

```
tabyldieva.complaint_cases ─(журнал WAL, pgoutput)─> Debezium source ─> Kafka topic tabyldieva_site.tabyldieva.complaint_cases
                                                                        └─> JDBC sink (upsert, delete) ─> tabyldieva_ods.ods_complaint_cases
```

## Кто что делает
| Шаг | Кто | Файл |
|---|---|---|
| Проверка стенда | студентка | `sql/00_preflight.sql` |
| Схема и таблица-источник | студентка (владелец схемы `tabyldieva`) | `sql/01_source_tables.sql`, `sql/02_seed_source.sql` |
| Отдельная роль `debezium` с правом REPLICATION | **администратор**, по желанию (на нашем стенде не понадобилась) | `sql/03a_cdc_role.sql` |
| Права для роли и публикация | студентка (нужно право CREATE на базу) | `sql/03_cdc_access.sql` |
| Коннекторы | студентка (под своей ролью `tabyldieva`; пароль вводится один раз) | `connectors/*.template`, `scripts/` |
| Изменения для показа | студентка | `sql/04_change_scenario.sql` |

Что известно о стенде: `wal_level = logical`, Debezium 2.7.3, Kafka Connect `http://kafka-connect:8083`, брокер `kafka:19092`. Проверено 23.10.2026: коннекторы работают под ролью владельца таблицы `tabyldieva`, `verify_cdc.py` показывает совпадение, UPDATE, INSERT и DELETE доезжают до приёмника. Отдельная роль `debezium` не понадобилась. В промышленной установке коннектору заводят отдельную роль с минимальными правами (REPLICATION и SELECT на нужные таблицы), для этого есть `sql/03a_cdc_role.sql`.

## Порядок (из папки `~/work/dags/final_project`)
Пароль Postgres вводится один раз на терминал: `read -s -p "PG password: " PGPASSWORD; echo; export PGPASSWORD`, плюс `export PGHOST=postgres PGUSER=tabyldieva PGDATABASE=aiacademy`.

```bash
# 0. разведка (только читает): wal_level, мои права, чужие слоты
psql -f cdc/sql/00_preflight.sql

# 1-2. источник и стартовые данные
psql -v ON_ERROR_STOP=1 -f cdc/sql/01_source_tables.sql
psql -v ON_ERROR_STOP=1 -f cdc/sql/02_seed_source.sql

# 3. права и публикация (cdc_user: под какой ролью работает коннектор; у нас tabyldieva)
# Отдельную роль debezium создаёт администратор через cdc/sql/03a_cdc_role.sql, тогда здесь -v cdc_user=debezium
psql -v ON_ERROR_STOP=1 -v cdc_user=tabyldieva -f cdc/sql/03_cdc_access.sql

# 4. переменные (пароль спросит один раз, в файлы не пишет) и коннекторы
. cdc/scripts/env.sh
python3 cdc/scripts/render_config.py cdc/connectors/source.json.template > /tmp/cdc_source.json
python3 cdc/scripts/render_config.py cdc/connectors/sink.json.template   > /tmp/cdc_sink.json
bash cdc/scripts/connect.sh ping
bash cdc/scripts/connect.sh create /tmp/cdc_source.json
bash cdc/scripts/connect.sh create /tmp/cdc_sink.json     # пароли в ответе заменяются на ***; всё равно не копируйте вывод в чаты и тикеты
rm -f /tmp/cdc_source.json /tmp/cdc_sink.json         # в готовых json лежат пароли
bash cdc/scripts/connect.sh status complaint-cases-source-tabyldieva    # ждём state RUNNING у коннектора и задачи
bash cdc/scripts/connect.sh status sink-tabyldieva

# 5. сверка после снапшота: число строк и контрольная сумма совпадают
python3 -I cdc/scripts/verify_cdc.py
```

## Показ для комиссии (ФТ-9)
```bash
psql -c "update tabyldieva.complaint_cases set status = 'closed_relief' where complaint_id = (select complaint_id from tabyldieva.complaint_cases where status = 'in_progress' limit 1) returning complaint_id, status"
# подождать до 30 секунд, затем (подставить complaint_id):
psql -c "select complaint_id, status, updated_at from tabyldieva_ods.ods_complaint_cases where complaint_id = <номер>"
```
Весь сценарий (UPDATE, INSERT, DELETE) в `sql/04_change_scenario.sql`; после него запустить `verify_cdc.py`.
Мониторинг слота: `psql -f cdc/sql/05_slot_monitor.sql` (`wal_status` должен быть `reserved` или `extended`).

Автоматически этот слой пересобирает DAG `final_tabyldieva_cdc_integration` (раз в час; включить в списке DAG после того, как коннекторы RUNNING; если dbt недоступен воркеру, вторая задача пропускается, первая пишет проверку приёмника в `etl_load_log`, `source = cdc_health`).
Слой CDC в dbt руками (после `04_change_scenario.sql`): `cd dbt/tabyldieva_dwh && dbt build -s tag:cdc`. В `tabyldieva_dbt_snapshots.complaint_cases_snapshot`
у изменённого обращения две версии (старая закрыта по `dbt_valid_to`), `tabyldieva_dbt.audit_cdc_vs_batch` показывает, что батч (DAG) ещё не знает
о смене статуса, а удалённое обращение закрывает свою версию в снимке (`hard_deletes: invalidate`).

## Если Kafka недоступна
`sql/10_sink_tables.sql` создаёт и один раз наполняет `ods_complaint_cases` обычным INSERT. **Это не CDC**, а запасной вариант, чтобы собрать dbt и проверки. Использовать только с оговоркой «Debezium на стенде недоступен»; на защите не выдавать за CDC.

## Уборка после защиты (забытый слот копит журнал на диске)
```bash
bash cdc/scripts/connect.sh delete sink-tabyldieva
bash cdc/scripts/connect.sh delete complaint-cases-source-tabyldieva
psql -f cdc/sql/99a_drop_slot.sql        # роль с REPLICATION или администратор
psql -f cdc/sql/99b_drop_publication.sql
```

## Типичные ошибки
- `permission denied for database` на шаге 3: нет права CREATE на базу, публикацию создаёт администратор (передать ему `03_cdc_access.sql`).
- Коннектор FAILED: `bash cdc/scripts/connect.sh status <имя>` покажет trace. Частые причины: неверный пароль роли, нет права SELECT на таблицу, публикации нет.
- `create` вернул 409: коннектор с таким именем уже есть (например, `sink-<логин>` с урока); посмотрите `curl -s $CONNECT_URL/connectors` и удалите свой старый, чужой не трогайте.
- Коннектор создался, а `tasks` пусто: подождите 15 секунд и повторите `status`.
- В приёмнике нет строк: смотреть status sink-коннектора; в шаблоне `insert.mode = upsert` и `primary.key.mode = record_key` обязательны.
- Не сходится число строк после UPDATE: `REPLICA IDENTITY FULL` стоит в `01_source_tables.sql` (в событии есть before и after).
