# Карта: день курса -> что сделано -> как показать

| День курса | Что в проекте | Файлы | Как показать (команда / экран) |
|---|---|---|---|
| 1. Введение, архитектура, требования | цель, бизнес-вопросы, источники, ограничения, схема архитектуры, честные оговорки | `docs/00_requirements_architecture.md` | открыть документ, пройти схему |
| 2. Схема данных и слои | слои stg/ods/datamart, ключи, режимы загрузки, словарь полей | `docs/01_schema_design.md`, `sql/01..10, 12, 22..24` | `\dt` по схемам `<login>_stg/_ods/_datamart` |
| 3. Источники и структура проекта | карточки источников (CFPB, FDIC, справочники), что проверено, структура папок | `docs/sources.md`, `README.md`, `tools/probe_api.py` | `python3 tools/probe_api.py ...` |
| 4. Ingestion-слой | клиент API: ретраи, курсор, один продукт за запрос, проверки, curl из-за 403; клиент FDIC по кварталам | `ingestion/load_cfpb.py`, `cfpb_http.py`, `common.py`, `fdic_fin_pipeline.py` | `python3 ingestion/load_cfpb.py --ds 2026-09-01 --dry-run` |
| 5. Загрузка сырых данных в staging | запись в stg, delete+insert по дню, журнал, сверка с API, DQ-запросы, досинхронизация поздних жалоб | `sql/02, 04, 12, 22`, `tools/reconcile_api.py`, `tools/reconcile_bureau.py`, `ingestion/resync_pipeline.py` | `sql/11_window_checks.sql`, `select * from etl_load_log` |
| 6. ETL/ELT на Python/SQL | stg→ods→dm на Python+SQL (Spark по ФТТ не обязателен, не используется) | `ingestion/cfpb_pipeline.py`, `bureau_pipeline.py`, `sql/05..10, 23, 24` | витрины `dm_*` |
| 7. Оркестрация в Airflow | `final_tabyldieva_pipeline` и `final_tabyldieva_datamart` (ФТТ: загрузка, затем витрины и ClickHouse, запуск по TriggerDagRunOperator), плюс исторический `cfpb_final` и дополнительные бюро, FDIC (месяц), досинхронизация (неделя); ретраи, `max_active_runs=1` | `dag/final_tabyldieva_*.py`, `dag_cfpb_final.py`, `dag_cfpb_bureau.py`, `dag_fdic_financials.py`, `dag_cfpb_resync.py` | Grid и Graph в Airflow UI |
| 9. dbt | структура как у ментора: intermediate + marts, seeds, snapshots, incremental-модель, тесты (свои и сверка с витринами DAG), слой CDC под тегом `cdc`, variants | `dbt/tabyldieva_dwh/` | `dbt build --exclude tag:cdc` (после CDC: `dbt build`) |
| 10. CDC | Debezium + Kafka: изменения `tabyldieva.complaint_cases` → `tabyldieva_ods.ods_complaint_cases`; интеграция в общий контур: DAG `final_tabyldieva_cdc_integration` (проверка приёмника, `dbt build --select tag:cdc`) | `cdc/`, `dag/final_tabyldieva_cdc_integration.py` | `cdc/README.md`, раздел «Показ для комиссии» |
| 8. Загрузка в PostgreSQL / ClickHouse | PostgreSQL: stg/ods/dm; ClickHouse: delete+insert дня + сверка числа строк, таблицы `cfpb_complaints` и `cfpb_bureau` | `cfpb_pipeline.py`, `bureau_pipeline.py`, `ingestion/clickhouse_load.py`, `sql/08, 14` | таск `load_clickhouse`; `select count() ... final` |

## Статус проверок: контур, проверенный на стенде (17.10.2026)
- Проверено на реальных данных в твоей базе: покрытие банковского контура 1096 дней без дыр; stg = ods = dm; контур бюро (92 дня) stg = ods;
  сверка с API по дням; досинхронизация (57 дней найдено, после неё 0 расхождений); FDIC: 14 кварталов; число строк в ClickHouse равно ods по обоим контурам.
- Проверено на временной базе с имитацией данных (не на настоящей): запросы показа `sql/25_demo_queries.sql` (синтаксис и имена таблиц).
- НЕ проверялось: влияние слияний банков на сопоставление `company_bank_map`.

## Статус на 23.10.2026: что проверено на стенде
Проверено на стенде (в твоей базе, есть вывод в терминале):
- Батч: `final_tabyldieva_pipeline` и `final_tabyldieva_datamart` за 2026-09-14 зелёные; витрины пересобраны на всё окно, 855 110 жалоб.
- ClickHouse: суммы витрины в Postgres и ClickHouse равны (855 110).
- CDC: коннекторы источника и приёмника `RUNNING`; `cdc/scripts/verify_cdc.py` показывает совпадение (28 240 строк, контрольная сумма); UPDATE, INSERT и DELETE доезжают до `tabyldieva_ods.ods_complaint_cases`.
- dbt: `dbt build` целиком (с CDC) = 83 узла, `PASS=83 WARN=0 ERROR=0`; сверочные тесты dbt и витрин DAG зелёные; в снимке `complaint_cases_snapshot` у изменённого обращения две версии статуса.
НЕ проверено на стенде: DAG `final_tabyldieva_cdc_integration`, идемпотентность перезапуска (`sql/90_acceptance_checks.sql` до и после Clear), Superset. Обновляйте эту таблицу после каждого шага.

| Требование ФТТ | Статус на стенде |
|---|---|
| ФТ-1…ФТ-3, ФТ-10 (источники, stg, ods, проверки) | работает: прогон за 2026-09-14 зелёный, окно загружено |
| ФТ-4 (журнал) | работает: `etl_load_log` заполняется |
| ФТ-5, ФТ-6 (витрины, запуск DAG витрин) | работает: DAG витрин запускается основным DAG |
| ФТ-7 (ClickHouse) | работает: суммы Postgres и ClickHouse равны |
| ФТ-8 (dbt) | работает: 83 узла, ошибок и предупреждений нет |
| ФТ-9 (CDC) | работает (UPDATE, INSERT, DELETE); DAG интеграции ждёт запуска |
| НФТ-2 (секреты) | ждёт проверки `grep` по проекту; пароль стенда стандартный |
