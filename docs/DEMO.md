# Как проверить проект

Команды выполняются в терминале JupyterHub из папки `~/work/dags/final_project`. Перед началом:
```bash
export PGHOST=postgres PGUSER=tabyldieva PGDATABASE=aiacademy
read -s -p "PG password: " PGPASSWORD; echo; export PGPASSWORD
S=${PGUSER}
```
`ds` ниже — дата, которую выбирает проверяющий.

## Проверки по требованиям
| № | Что проверить | Как |
|---|---|---|
| ФТ-1 | паспорт источников | `docs/passport.md` |
| ФТ-2 | загрузка за дату | Airflow: `final_tabyldieva_pipeline` → ▶ Trigger DAG w/ config → Logical date = `ds`. Затем `select count(*), count(_source_file), count(_loaded_at) from tabyldieva_stg.stg_cfpb_complaints where _ds = 'ds';` и сверка с API: `python3 tools/reconcile_api.py --start ds --end ds`. Ставка ФРС: одна строка в `tabyldieva_stg.stg_fred_dff` за `ds`. Если запуск с такой датой уже есть, нажать Clear у него или указать время `T12:00:00+00:00` |
| ФТ-3 | типы и ключи ods | `\d tabyldieva_ods.ods_complaints`, `\d tabyldieva_ods.ods_fdic_financials` |
| ФТ-4 | журнал | `select * from tabyldieva_stg.etl_load_log order by id desc limit 15;` |
| ФТ-5 | вопрос одним запросом | запросы из `docs/data_model.md`, раздел 8, и сверка двух витрин оттуда же |
| ФТ-6 | DAG витрин запускается сам | после ▶ основного DAG в `final_tabyldieva_datamart` появляется запуск с той же Logical date |
| ФТ-7 | ClickHouse = Postgres | Postgres: `select sum(complaints_cnt) from tabyldieva_datamart.dm_complaints_daily;` ClickHouse: `read -s CHP; curl -s --user admin:$CHP "http://clickhouse:8123/" --data-binary "select sum(complaints_cnt) from tabyldieva.dm_complaints_daily final"`. Повторить после Clear того же дня |
| ФТ-8 | dbt | `cd dbt/tabyldieva_dwh && dbt build -s dm_complaints_daily` — модель и тесты зелёные, среди них `assert_dbt_matches_dag_daily` |
| ФТ-9 | CDC | `update tabyldieva.complaint_cases set status = 'closed_relief' where complaint_id = <id>;` подождать до 30 секунд, `select status from tabyldieva_ods.ods_complaint_cases where complaint_id = <id>;` статус коннекторов: `bash cdc/scripts/connect.sh status sink-tabyldieva` |
| ФТ-10 | красная проверка | `update tabyldieva_ods.ods_complaints set company = '' where complaint_id = <id за ds>;` → Clear задачи `dq_ods` → красная, в журнале «пустые company/product=1». Вернуть название, Clear → зелёная. В dbt: `update tabyldieva_dbt.dm_company_month set company_type = null where ctid = (select ctid from tabyldieva_dbt.dm_company_month limit 1);` → `dbt test -s dm_company_month` красный → `dbt build -s dm_company_month` чинит |
| НФТ-1 | повтор даты | `psql -v ds=ds -v stg=${S}_stg -v ods=${S}_ods -v dm=${S}_datamart -f sql/90_acceptance_checks.sql`, Clear запуска, снова тот же запрос: числа не меняются |
| НФТ-2 | пароли | `grep -rin "password" . --exclude-dir=.git` — только имена переменных, без значений |
| НФТ-4 | недоступный API | ▶ основного DAG, дата, которой ещё нет в stg, поле `cfpb_base` = `https://localhost:9/x` → `load_stg_complaints` красная после повторов, в stg за эту дату 0 строк. Параметр действует только на этот запуск |
| НФТ-5 | скорость | время запуска основного DAG и DAG витрин в Airflow (вкладка Gantt): около минуты на дату |
| НФТ-6 | с нуля по README | раздел «Как поднять с нуля» в `README.md` |

## Что лежит в слоях
```bash
psql -f sql/37_show_layers.sql | less -S
```
Файл выводит по экрану на слой: все таблицы со строками и размером, stg, ods и справочники, витрины, dbt, CDC, журнал, метрики
качества.

## Контрольные числа на 29.10.2026
| Что | Значение |
|---|---|
| Жалобы в ods | 869 218, с 2023-10-01 по 2026-10-13, 1 109 дней без пропусков |
| Витрины `dm_complaints_daily`, `dm_company_month`, `dm_state_month`, `dm_type_daily` | по 869 218 жалоб, в Postgres и ClickHouse одинаково |
| `dm_bank_size_quarter` | 264 строки, 11 кварталов |
| Обращения CDC | 28 240 |
| Метрики качества | 33, все в норме |
| Красный запуск основного DAG | один, за 2026-10-13T00:00: это проверка НФТ-4 с недоступным адресом API |

## Порядок показа
1. Задача и вопросы бизнеса — README.
2. Источники — `docs/passport.md`.
3. Слои и DAG — Airflow, Graph основного DAG и DAG витрин; запуск за дату.
4. Данные в слоях — `sql/37_show_layers.sql`.
5. Журнал и проверки — `etl_load_log`, `sql/90_acceptance_checks.sql`.
6. CDC — изменить статус обращения и увидеть его в ods.
7. dbt — `dbt build`, тесты зелёные.
8. Дашборд Superset — пять вкладок, фильтры слева, вкладка «Качество данных».
