# tabyldieva_dwh: dbt-слой финального проекта

Слой transform: источник `tabyldieva_ods` (его заполняют DAG и приёмник CDC) → модели → витрины в схеме `tabyldieva_dbt`.
Структура папок повторяет каркас ментора (`olist_dwh`): `models/` (sources, intermediate, marts), `seeds/`, `snapshots/`,
`tests/` (+ `generic/`), `variants/`, `profiles/`.

## Установка (версии закреплены; на стенде уже стоят dbt-core 1.12.4 и dbt-postgres 1.11.0, они тоже проверены)
    pip install "dbt-core==1.11.16" "dbt-postgres==1.10.2"

## Запуск (из папки final_project/dbt/tabyldieva_dwh)
    export DBT_HOST=postgres DBT_PORT=5432 DBT_USER=$PGUSER DBT_DBNAME=aiacademy
    read -s -p "Пароль: " DBT_PASSWORD; echo; export DBT_PASSWORD
    export DBT_PROFILES_DIR=$PWD/profiles
    dbt debug                          # проверка подключения
    dbt build --exclude tag:cdc        # пока CDC не настроен: seed + модели + снимок + тесты
    dbt build                          # после настройки CDC: то же плюс слой CDC

Ожидаем `ERROR=0` и `WARN=0`. Всего 83 узла (2 seed, 2 снимка, 9 моделей, 70 тестов), без CDC 62. Тест
`relationships_dm_state_month_state__state_code__ref_us_states_` оставлен с `severity: warn`: он ловит коды штатов, которых нет в
справочнике. Первая сборка на реальных данных нашла одно такое значение (CFPB присылает для Малых отдалённых островов США полное название
вместо кода, 48 жалоб), `int_complaints_enriched` приводит его к `UM`. Новый нестандартный код в будущем даст `WARN`: это замечание к
источнику, а не поломка. Первый раз и для приёмки ФТ-8 запускать **целиком** (`dbt build`): выборочный
`-s dm_complaints_daily` на пустой схеме упадёт, потому что тесты тянут соседние модели. После полной сборки `-s` работает.

## Структура
- `models/sources.yml`: источники `ods` (DAG), `cdc` (приёмник Debezium, метка `cdc`), `datamart` (витрины DAG для сверки) и тесты на них
- `models/intermediate/`: общий шаг (view). `int_complaints_enriched`: жалоба + тип компании + название штата + флаги;
  `int_cases_current` (CDC): обращение «как сейчас», батч и поток через full outer join
- `models/marts/`: `dim_calendar` и витрины: `dm_complaints_daily` (день x продукт), `dm_company_month` (месяц x компания),
  `dm_state_month` (месяц x штат), все table; `dm_type_daily` (день x тип компании) **инкрементальная**; CDC: `dm_open_cases`, `audit_cdc_vs_batch`
- `models/marts/schema.yml`, `schema_cdc.yml`: описания и тесты моделей (CDC-модели отдельным файлом)
- `seeds/`: справочники `us_states` (штаты) и `complaint_status` (статус обращения: открыто или закрыто)
- `snapshots/`: история справочника компаний (`dim_company_snapshot`) и история статусов обращений
  (`complaint_cases_snapshot`, CDC), SCD2
- `tests/generic/`: свои тесты `non_negative`, `unique_combination_of_columns`
- `tests/*.sql`: сверочные тесты. `assert_dbt_matches_dag_daily` и `assert_dbt_matches_dag_month`: результат dbt равен витрине
  DAG; `assert_marts_complaints_equal`: сумма жалоб одинакова в ods и в витринах; `assert_type_daily_matches_ods_recent`:
  инкрементальная витрина сошлась с ods за последние 30 дней; `assert_timely_split_valid`; `assert_open_cases_total` (CDC)
- `variants/`: версии для разбора, в сборку не входят: `dm_type_daily_v1_table.sql` (первая версия без инкремента),
  `reconcile_dbt_vs_dag.sql` (ручная сверка dbt и DAG через psql)

Схемы в базе: `tabyldieva_dbt` (модели и seed), `tabyldieva_dbt_snapshots` (снимки). Права на создание этих двух схем
нужны владельцу `tabyldieva`; если их нет, написать администратору. Перед `dbt build` в ods должны быть данные
(DAG `final_tabyldieva_pipeline` и справочники); для слоя CDC: таблица `ods_complaint_cases` (коннектор Debezium).

## Теги и CDC
Всё, что читает `ods_complaint_cases`, помечено `cdc`: источник `cdc`, модели `int_cases_current`, `dm_open_cases`,
`audit_cdc_vs_batch`, снимок `complaint_cases_snapshot`, тест `assert_open_cases_total`. Пока коннектора нет:
`dbt build --exclude tag:cdc`. Свежесть приёмника: `dbt source freshness --select source:cdc` (в `dbt build` не входит;
данные синтетические, поэтому сутки без изменений дадут `error`, это не поломка).

## Что показать при приёмке
- ФТ-8: `dbt build --exclude tag:cdc` (или `dbt build`): все тесты зелёные, среди них `assert_dbt_matches_dag_daily`
- ФТ-10, вариант B: `update tabyldieva_dbt.dm_company_month set company_type = null where ctid = (select ctid from tabyldieva_dbt.dm_company_month limit 1);`
  → `dbt test -s dm_company_month` красный (`not_null_dm_company_month_company_type`) → `dbt build -s dm_company_month` лечит
- CDC: после `cdc/sql/04_change_scenario.sql` выполнить `dbt build -s tag:cdc`: в `tabyldieva_dbt_snapshots.complaint_cases_snapshot`
  у обращения две версии (закрытая и текущая), в `tabyldieva_dbt.audit_cdc_vs_batch` видно, что батч ещё не знает о смене статуса

## Table и incremental: что где и почему
- `dm_complaints_daily`, `dm_company_month`, `dm_state_month` пересобираются целиком (`table`). Первые две сверяются с витринами DAG,
  а CFPB публикует жалобы с опозданием, и DAG `cfpb_resync` исправляет дни задним числом, в том числе старше 30. Данных 0,86 млн строк, пересборка занимает секунды.
- `dm_type_daily` инкрементальная (`delete+insert` по ключу день x тип, `on_schema_change: fail`): первый запуск строит всё, дальше пересчитываются
  последние `lookback_days` = 30 дней. Показать: изменить жалобу последнего дня в ods → `dbt build -s dm_type_daily` → строка обновилась (в логе `INSERT 0 N` вместо полной пересборки).
  Если DAG `cfpb_resync` поправил дни старше 30: `dbt build --full-refresh -s dm_type_daily`. Версия без инкремента лежит в `variants/`.
