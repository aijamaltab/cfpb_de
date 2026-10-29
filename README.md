# Жалобы клиентов на банки США: хранилище данных на CFPB

Финальный проект курса Data Engineering. Автор: Айжамал Табылдиева, логин на стенде `tabyldieva`.

Я собираю жалобы клиентов на американские банки из открытого API CFPB, добавляю к ним размер банков (API FDIC), ставку ФРС и
справочники компаний (CSV), раскладываю всё по слоям stg → ods → datamart в PostgreSQL, копирую витрины в ClickHouse и показываю
результат в Superset. Изменения статусов обращений приходят через CDC (Debezium + Kafka). Данные американские, но схема подойдёт
любому банку, у которого есть поток обращений клиентов.

Документы: `docs/passport.md` (источники), `docs/data_model.md` (слои, таблицы, витрины, метрики), `docs/DEMO.md` (как проверить
проект по критериям приёмки).

## Какие вопросы закрывает проект
1. По каким продуктам и компаниям жалоб больше всего и как это меняется по дням и месяцам.
2. Как быстро компании отвечают (поле `timely`) и чем закрывают жалобы.
3. У каких банков жалоб больше при том же размере (жалоб на 1 млрд USD активов).
4. Сколько обращений висит «в работе» и как давно они открыты (по потоку CDC).

В жалобах нет сумм, поэтому все метрики — это количества, доли и сроки.

## Что сейчас лежит в хранилище (на 29.10.2026)
- 869 218 жалоб на 8 банковских продуктов за 1 109 дней, с 1 октября 2023 по 13 октября 2026, без пропущенных дней. Число
  совпадает в ods, во всех витринах и в ClickHouse.
- 14 кварталов отчётности FDIC (около 63,8 тыс. строк), витрина по размеру банков: 24 компании, 11 кварталов.
- 28 240 обращений в таблице CDC.

## Источники
| Тип | Источник | Таблица stg | Как грузится |
|---|---|---|---|
| API | CFPB Consumer Complaint Database | `stg_cfpb_complaints` | жалобы за день `ds` |
| API | FDIC `/financials`, показатели банков по кварталам | `stg_fdic_financials` | все кварталы целиком, отдельным DAG |
| CSV, событие с датой | ставка ФРС FRED `DFF`, `data/fred/DFF.csv` | `stg_fred_dff` | строка за день `ds` |
| CSV, справочник | `data/ref/dim_company.csv`, 60 компаний и их тип | `stg_dim_company` | целиком |
| CSV, справочник | `data/ref/company_bank_map.csv`, компания → банк FDIC | `stg_company_bank_map` | целиком |
| «База сайта» | `tabyldieva.complaint_cases`, статусы обращений | — | Debezium → Kafka → `ods_complaint_cases` |

У CFPB нет потока изменений, поэтому таблицу `complaint_cases` для CDC я сделала сама: обращения взяты из реальных жалоб, а статусы
меняет скрипт. Механика CDC настоящая, изменения синтетические.

## Как устроено
```
CFPB API ───> stg_cfpb_complaints ───> ods_complaints ─────────┐
FRED CSV ───> stg_fred_dff ──────────> ods_fred_dff ───────────┤  DAG final_tabyldieva_pipeline
CSV справочники ─> stg_dim_company, stg_company_bank_map ─> ods_dim_company, ods_company_bank
FDIC API ───> stg_fdic_financials ───> ods_fdic_financials ────┘
                                              │  TriggerDagRunOperator, та же дата
                                              v
       tabyldieva_datamart: dm_complaints_daily, dm_company_month, dm_bank_size_quarter   (DAG final_tabyldieva_datamart)
                                              │
                                              v
       ClickHouse tabyldieva.dm_* ──> Superset, дашборд «Жалобы на банки США»

CDC:  tabyldieva.complaint_cases ─Debezium─> Kafka ─sink─> tabyldieva_ods.ods_complaint_cases ─dbt─> dm_open_cases
dbt:  ods ─> tabyldieva_dbt: int_*, dm_state_month, dm_type_daily (инкрементальная), копии витрин DAG для сверки, тесты, снимки
Журнал всех шагов: tabyldieva_stg.etl_load_log.   Метрики качества: tabyldieva_datamart.dq_metrics
```
- **stg** — данные как пришли: все колонки `text` и служебные `_ds`, `_source_file`, `_loaded_at`.
- **ods** — правильные типы, понятные имена, первичный ключ.
- **datamart** — витрины для запросов и дашборда.

Везде работает delete + insert по дню (справочники перезаписываются целиком), поэтому повторный запуск даты не меняет число строк.

## DAG
| DAG | Когда | Что делает |
|---|---|---|
| `final_tabyldieva_pipeline` | каждый день; можно запустить за любую дату | API и CSV в stg, сверка числа жалоб с API, ods, проверки, затем запускает DAG витрин с той же датой |
| `final_tabyldieva_datamart` | его запускает основной DAG | витрины, проверки витрин, копия трёх витрин в ClickHouse со сверкой строк и сумм |
| `final_tabyldieva_refresh` | 00:30 UTC | перезагружает последние 28 дней, раз в неделю (ночь на четверг) 120 дней: так в хранилище попадают поздние жалобы и новые статусы ответа |
| `final_tabyldieva_dbt_marts` | 02:00 UTC | `dbt build` батч-витрин с тестами и копия двух dbt-витрин в ClickHouse |
| `final_tabyldieva_dq` | 03:00 UTC | метрики качества данных по шести измерениям, история в `dq_metrics` |
| `final_tabyldieva_cdc_integration` | каждый час | проверка приёмника CDC, `dbt build --select tag:cdc`, копия `dm_open_cases` в ClickHouse |

Ручной запуск основного DAG грузит ровно выбранную дату. Запуск по расписанию грузит день на 22 дня раньше своей даты: CFPB
публикует жалобу только после ответа компании или через 15 дней, поэтому за вчера API почти всегда отдаёт ноль.

Старые DAG первой версии (`cfpb_final` с историей 2023–2026, `cfpb_bureau_pipeline`, `cfpb_resync`, `fdic_financials_pipeline`)
стоят на паузе, `cfpb_final` и `cfpb_bureau_pipeline` включать нельзя: их таблицы удалены. FDIC публикует новый квартал раз в три месяца, тогда я запускаю `fdic_financials_pipeline` вручную.

## Поздние жалобы и статусы
CFPB дописывает прошлые дни задним числом и меняет статус ответа компании, пока жалоба в работе. Поля «дата изменения» у API нет,
а CDC к чужой базе не подключить, поэтому DAG `final_tabyldieva_refresh` просто перезагружает окно последних дней целиком.
Глубину окна я выбрала по замеру: доля жалоб «In progress» по месяцам была 36% за октябрь, 30% за сентябрь, 10,5% за август,
0,6% за июль и почти ноль раньше. Значит, статусы устаканиваются примерно за три месяца: 28 дней каждую ночь и 120 дней раз
в неделю. После первой перезагрузки (128 дней) доля «In progress» за август упала до 1,4%, за сентябрь до 19,3%.

## Качество данных
- В основном DAG: сверка числа жалоб за день с ответом API, проверки ods (пустые поля, чужая дата, продукт вне списка, жалоба
  в двух днях, объём дня) и проверки витрин (суммы витрин равны ods и друг другу). Красная проверка останавливает загрузку.
- dbt: `unique`, `not_null`, `accepted_values`, `relationships` и свои тесты, в том числе сверка dbt-витрин с витринами DAG.
- DAG `final_tabyldieva_dq` каждый день считает 33 метрики по слоям: полнота (`null_rate`), уникальность (`dup_rate`),
  валидность (`bad_rate`), согласованность (`mismatch`), точность (`drift` ClickHouse против Postgres, сверка с API, единицы
  измерения) и актуальность (`lag`). У каждой метрики есть порог, формулы лежат в `ingestion/dq_metrics.py`.

## Superset
Дашборд «Жалобы на банки США» читает витрины из ClickHouse. Пять вкладок: продукты и компании; скорость и итог ответа; размер
банка; обращения в работе (CDC); качество данных. Слева панель фильтров: период, продукт, компания, тип компании, статусы CDC
и измерение качества.

## Как поднять с нуля
Терминал JupyterHub, папка `~/work/dags/final_project`. Пароль вводится руками и в файлах не хранится:
```bash
export PGHOST=postgres PGUSER=tabyldieva PGDATABASE=aiacademy
read -s -p "PG password: " PGPASSWORD; echo; export PGPASSWORD
S=${PGUSER}
```
**1. Схемы и таблицы** (миграция `30` идёт сразу после журнала):
```bash
psql -v ON_ERROR_STOP=1 -v schema=${S}_stg -f sql/01_etl_load_log.sql
psql -v ON_ERROR_STOP=1 -v stg=${S}_stg -v ods=${S}_ods -v dm=${S}_datamart -v dm_old=${S}_dm -v meta=${S}_meta -f sql/30_final_migration.sql
psql -v ON_ERROR_STOP=1 -v schema=${S}_stg -f sql/02_stg_cfpb.sql
psql -v ON_ERROR_STOP=1 -v ods=${S}_ods -f sql/05_ods_complaints.sql
psql -v ON_ERROR_STOP=1 -v ods=${S}_ods -f sql/07_ods_reference.sql
psql -v ON_ERROR_STOP=1 -v dm=${S}_datamart -f sql/09_dm.sql
psql -v ON_ERROR_STOP=1 -v stg=${S}_stg -v ods=${S}_ods -f sql/22_fdic_financials.sql
psql -v ON_ERROR_STOP=1 -v stg=${S}_stg -v ods=${S}_ods -f sql/31_csv_tables.sql
psql -v ON_ERROR_STOP=1 -v ods=${S}_ods -v dm=${S}_datamart -f sql/24_dm_bank_size.sql
```
**2. Airflow.** Admin → Connections: `aiacademy_pg_tabyldieva` (Postgres; host `postgres`, schema `aiacademy`, login `tabyldieva`,
port 5432, пароль в форме). Admin → Variables для ClickHouse: `tabyldieva_ch_host`=clickhouse, `tabyldieva_ch_port`=8123,
`tabyldieva_ch_user`=admin, `tabyldieva_ch_password`, `tabyldieva_ch_db`=tabyldieva; `tabyldieva_ch_enabled`=1 ставится на шаге 6.

**3. Ставка ФРС** (ключ не нужен):
```bash
mkdir -p data/fred
curl -sS -m 60 "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFF&cosd=2023-10-01" -o data/fred/DFF.csv
head -3 data/fred/DFF.csv            # первая строка: observation_date,DFF
python3 tools/backfill_fred.py       # вся история ставки в stg и ods
```
**4. DAG.** Airflow стенда читает DAG из `~/work/dags/`, а папка `dag/` — исходники для git (она в `.airflowignore`, иначе DAG
найдётся дважды):
```bash
cp dag/final_tabyldieva_*.py ..
```
Через 1–2 минуты DAG появятся с префиксом `tabyldieva.`. Включить `final_tabyldieva_datamart` и `final_tabyldieva_pipeline`,
затем запустить основной DAG за дату: ▶ → Trigger DAG w/ config → Logical date. DAG витрин запустится сам. Пока ClickHouse не
настроен, задачи `load_clickhouse*` будут skipped, это нормально.

**5. Тип компании и ставка для старых дат** (один раз, после первого успешного запуска):
```bash
psql -v ON_ERROR_STOP=1 -v ods=${S}_ods -v dm=${S}_datamart -f sql/33_backfill_columns.sql
```
**6. ClickHouse.** Создать таблицы витрин и залить историю, потом поставить `tabyldieva_ch_enabled`=1:
```bash
read -s -p "CH password: " CHP; echo
curl -s --user admin:$CHP "http://clickhouse:8123/" --data-binary @sql/32_clickhouse_dm.sql; unset CHP
read -s -p "CH password: " CH_PASSWORD; echo; export CH_PASSWORD
export CH_HOST=clickhouse CH_PORT=8123 CH_USER=admin CH_DB=tabyldieva
python3 tools/clickhouse_backfill_dm.py
python3 tools/clickhouse_backfill_marts.py --create
python3 tools/clickhouse_backfill_marts.py
```
**7. dbt, CDC и остальные DAG.** dbt: `dbt/tabyldieva_dwh/README.md`; CDC: `cdc/README.md`. После этого включить
`final_tabyldieva_cdc_integration`, `final_tabyldieva_dbt_marts`, `final_tabyldieva_refresh` и `final_tabyldieva_dq`.
Таблицу `dq_metrics` DAG качества создаёт сам.

**8. Проверка:**
```bash
psql -v ds=2026-09-14 -v stg=${S}_stg -v ods=${S}_ods -v dm=${S}_datamart -f sql/90_acceptance_checks.sql
psql -f sql/37_show_layers.sql | less -S       # что лежит в каждом слое
```
На пустом стенде история грузится запусками основного DAG по дням или DAG `final_tabyldieva_refresh` с параметрами `start`
и `end` (до 130 дней за раз).

## Имена
| Что | Имя |
|---|---|
| Схемы Postgres (база `aiacademy`) | `tabyldieva_stg`, `tabyldieva_ods`, `tabyldieva_datamart`, `tabyldieva_dbt`, `tabyldieva_dbt_snapshots`; «база сайта» `tabyldieva` |
| Airflow Connection | `aiacademy_pg_tabyldieva`, пароль только там |
| ClickHouse | база `tabyldieva`, таблицы с теми же именами, что витрины |
| Коннекторы Kafka Connect | `complaint-cases-source-tabyldieva`, `sink-tabyldieva` |

## Ограничения
- Только около 43% жалоб относятся к банкам из FDIC (24 компании, 25 банков). Остальное — финтех, бюро, сервисеры и кредитные
  союзы, их в FDIC нет.
- «Жалоб на 1 млрд активов» — грубая замена «жалоб на клиента», числа клиентов в открытых данных нет. Слияния банков я не проверяла.
- Ставка ФРС стоит в витрине как контекст дня, причинную связь с жалобами я не утверждаю.
- Последние недели всегда неполные: CFPB продолжает дописывать жалобы и менять статусы.

## Структура
```
dag/        final_tabyldieva_pipeline, _datamart, _refresh, _dbt_marts, _dq, _cdc_integration
ingestion/  загрузка API и CSV (load_cfpb.py, cfpb_http.py, csv_sources.py, fdic_fin_pipeline.py), слои и витрины
            (cfpb_pipeline.py, datamart_pipeline.py), ClickHouse (clickhouse_load.py), качество (dq_metrics.py), CDC и dbt
sql/        таблицы по слоям, ClickHouse, проверки (90_acceptance_checks.sql), просмотр слоёв (37_show_layers.sql)
dbt/        tabyldieva_dwh: models (intermediate, marts), seeds, snapshots, tests, variants, profiles без пароля
cdc/        sql для источника и приёмника, шаблоны коннекторов, скрипты
data/       fred/DFF.csv, ref/dim_company.csv, ref/company_bank_map.csv
docs/       passport.md, data_model.md, DEMO.md, api_sample.json (пример ответа API)
tools/      сверка с API, заливка истории в ClickHouse, загрузка ставки ФРС
```

## Что дальше
- Заменить ручной запуск FDIC на ежеквартальное расписание.
- Алерт в мессенджер, когда DAG качества становится красным.
- Генерация однотипных DAG через dag-factory.
