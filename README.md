# Мониторинг жалоб клиентов на банки (CFPB) — финальный проект Data Engineering

Проект собирает жалобы клиентов американских банков из открытого API CFPB, добавляет ставку ФРС (CSV) и справочники компаний (CSV),
кладёт всё в многослойное хранилище PostgreSQL, считает витрины, копирует витрину в ClickHouse и получает изменения статусов обращений
через CDC (Debezium + Kafka). Данные американские, архитектура от страны не зависит: так же строится мониторинг жалоб для любого банка.

Требования ментора (ФТТ): `docs/passport.md` (источники), `docs/data_model.md` (модель данных). Сценарий защиты: `docs/DEMO.md`.

## Бизнес-вопросы
1. По каким продуктам и компаниям жалоб больше всего и как это меняется по дням и месяцам?
2. Как быстро компании отвечают (`timely`) и чем закрывают жалобы?
3. У каких банков жалоб больше при том же размере (жалоб на 1 млрд USD активов)?
4. Сколько обращений остаётся «в работе» и как давно они открыты (по потоку изменений CDC)?

Сумм в жалобах нет: только счётчики, доли и сроки.

## Источники данных (подробно: `docs/passport.md`)
| Тип | Источник | Куда | Как грузится |
|---|---|---|---|
| API | CFPB Consumer Complaint Database | `stg_cfpb_complaints` | события за день `ds` |
| API | FDIC `/financials` (активы банков по кварталам) | `stg_fdic_financials` | DAG `fdic_financials_pipeline`, раз в месяц |
| CSV, событие с датой | ставка ФРС FRED `DFF`, `data/fred/DFF.csv` | `stg_fred_dff` | строка за день `ds` |
| CSV, справочник | `data/ref/dim_company.csv` (60 компаний, тип) | `stg_dim_company` | целиком |
| CSV, справочник | `data/ref/company_bank_map.csv` (компания → банк FDIC) | `stg_company_bank_map` | целиком |
| «База сайта» (CDC) | `tabyldieva.complaint_cases` (статусы обращений, имитация) | `tabyldieva_ods.ods_complaint_cases` | Debezium → Kafka → sink |

Объёмы банковского контура CFPB (проверено 23.10.2026): 855 110 жалоб за 1 окт 2023 — 30 сен 2026, 1096 дней без дыр. Число немного меняется: CFPB публикует жалобы с опозданием, и DAG `cfpb_resync` добавляет поздние.
Отдельный контур кредитных бюро (1 909 762 жалобы, 92 дня) — дополнение сверх минимума.

## Архитектура
```
CFPB API ──────────────> stg_cfpb_complaints ──> ods_complaints ──┐
CSV FRED DFF ──────────> stg_fred_dff ─────────> ods_fred_dff ────┤
CSV справочники ───────> stg_dim_company, stg_company_bank_map ─> ods_dim_company, ods_company_bank ┤
FDIC API ──────────────> stg_fdic_financials ──> ods_fdic_financials ┘
                                                                  │  DAG final_tabyldieva_pipeline  (stg -> ods -> проверки)
                                                                  v  TriggerDagRunOperator, та же дата
                                  <логин>_datamart: dm_complaints_daily, dm_company_month, dm_bank_size_quarter
                                                                  │  DAG final_tabyldieva_datamart  (витрины -> проверки -> ClickHouse)
                                                                  v
                                     ClickHouse: tabyldieva.dm_complaints_daily  (та же таблица, те же суммы)

CDC:  tabyldieva.complaint_cases ─Debezium─> Kafka ─sink─> tabyldieva_ods.ods_complaint_cases (текущее состояние)
dbt:  tabyldieva_ods ──> tabyldieva_dbt (int_*, dim_calendar, dm_complaints_daily, dm_company_month, dm_state_month, dm_type_daily; CDC: dm_open_cases, audit_cdc_vs_batch) + тесты + снимки
Журнал всех шагов: <логин>_stg.etl_load_log
```
Слои: `stg` — как пришло, все колонки text + `_ds`, `_source_file`, `_loaded_at`; `ods` — типы и первичные ключи; `datamart` — витрины.
Режим загрузки везде: delete + insert по дню (`ds`) или целиком для справочников; повторный запуск даты не меняет число строк.

## Имена (все с логином `tabyldieva`)
| Что | Имя |
|---|---|
| Схемы PostgreSQL (база `aiacademy`) | `tabyldieva_stg`, `tabyldieva_ods`, `tabyldieva_datamart`; «база сайта» для CDC: `tabyldieva`; dbt: `tabyldieva_dbt`; для проверки ментора: `tabyldieva_meta` (отражение журнала) |
| DAG | `final_tabyldieva_pipeline`, `final_tabyldieva_datamart` (+ дополнительные: `cfpb_final`, `cfpb_bureau_pipeline`, `fdic_financials_pipeline`, `cfpb_resync`) |
| Airflow Connection | `aiacademy_pg_tabyldieva` (тип Postgres; пароль только там) |
| ClickHouse | база `tabyldieva`, таблица `dm_complaints_daily` |
| Коннекторы Kafka Connect | `complaint-cases-source-tabyldieva`, `sink-tabyldieva` |

## Быстрый старт с нуля (терминал JupyterHub, из папки `~/work/dags/final_project`)
Пароль вводится вручную и в файлах не хранится:
```bash
export PGHOST=postgres PGUSER=tabyldieva PGDATABASE=aiacademy
read -s -p "PG password: " PGPASSWORD; echo; export PGPASSWORD
```
**Шаг 1. Схемы и таблицы** (порядок важен: миграция `30` идёт сразу после журнала и до витрин; на чистом стенде она просто создаёт схемы):
```bash
S=${PGUSER}
psql -v ON_ERROR_STOP=1 -v schema=${S}_stg -f sql/01_etl_load_log.sql
psql -v ON_ERROR_STOP=1 -v stg=${S}_stg -v ods=${S}_ods -v dm=${S}_datamart -v dm_old=${S}_dm -v meta=${S}_meta -f sql/30_final_migration.sql
psql -v ON_ERROR_STOP=1 -v schema=${S}_stg -f sql/02_stg_cfpb.sql
psql -v ON_ERROR_STOP=1 -v ods=${S}_ods    -f sql/05_ods_complaints.sql
psql -v ON_ERROR_STOP=1 -v ods=${S}_ods    -f sql/07_ods_reference.sql
psql -v ON_ERROR_STOP=1 -v dm=${S}_datamart -f sql/09_dm.sql
psql -v ON_ERROR_STOP=1 -v stg=${S}_stg -v ods=${S}_ods -f sql/22_fdic_financials.sql
psql -v ON_ERROR_STOP=1 -v stg=${S}_stg -v ods=${S}_ods -f sql/31_csv_tables.sql   # на базе, созданной до 23.10.2026, ещё: -v ods=${S}_ods -f sql/34_fred_carry.sql
psql -v ON_ERROR_STOP=1 -v ods=${S}_ods -v dm=${S}_datamart -f sql/24_dm_bank_size.sql
```

**Шаг 2. Airflow:** Admin → Connections → добавить `aiacademy_pg_tabyldieva` (Postgres; Host `postgres`, Schema `aiacademy`, Login `tabyldieva`, Port 5432, пароль вводится в форме).
Admin → Variables для ClickHouse: `tabyldieva_ch_host`=clickhouse, `tabyldieva_ch_port`=8123, `tabyldieva_ch_user`=admin, `tabyldieva_ch_password`=<пароль>, `tabyldieva_ch_db`=tabyldieva (переменную `tabyldieva_ch_enabled`=1 добавить на шаге 6).

**Шаг 3. CSV-данные.** Справочники лежат в `data/ref/`. Ставка ФРС скачивается одной командой (ключ не нужен):
```bash
mkdir -p data/fred
curl -sS -m 60 "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFF&cosd=2023-10-01" -o data/fred/DFF.csv
head -3 data/fred/DFF.csv        # первая строка: observation_date,DFF
python3 tools/backfill_fred.py   # один раз: вся история ставки в stg и ods (DAG потом добавляет по дню)
```
Если файла не хватает до даты запуска, задача `load_stg_fred` сама докачивает ряд той же командой; при ошибке сети повторите `curl` вручную.

**Шаг 4. Положить DAG туда, откуда их читает Airflow, и включить.** Стенд читает DAG из `~/work/dags/` (рядом лежат остальные `*.py`), а в `dag/` хранится исходник для git: эта папка перечислена в `.airflowignore`, чтобы один и тот же DAG не нашёлся дважды.
```bash
cp dag/final_tabyldieva_*.py ..      # из ~/work/dags/final_project
```
Через 1–2 минуты в списке DAG появятся `tabyldieva.final_tabyldieva_pipeline`, `tabyldieva.final_tabyldieva_datamart` и `tabyldieva.final_tabyldieva_cdc_integration` (префикс `tabyldieva.` добавляет стенд через `student_dag`). Включить переключателями `final_tabyldieva_datamart`, затем `final_tabyldieva_pipeline` (новые DAG создаются на паузе; `cdc_integration` пока не включать).
Запуск за дату: `final_tabyldieva_pipeline` → Trigger DAG w/ config → задать Logical date. DAG витрин запустится сам с той же датой.
Пока ClickHouse не настроен (шаг 6), последняя задача `load_clickhouse` будет в статусе skipped: это нормально.

**Шаг 5. История колонок витрин** (один раз, после первого успешного прогона, потому что справочники загружает DAG):
```bash
psql -v ON_ERROR_STOP=1 -v ods=${S}_ods -v dm=${S}_datamart -f sql/33_backfill_columns.sql   # ставка и тип компании для старых дат
```
**Шаг 6. ClickHouse:** создать таблицу витрины и один раз залить историю, затем включить задачу (Variable `tabyldieva_ch_enabled` = 1):
```bash
read -s -p "CH password: " CHP; echo
curl -s --user admin:$CHP "http://clickhouse:8123/" --data-binary @sql/32_clickhouse_dm.sql ; unset CHP
read -s -p "CH password: " CH_PASSWORD; echo; export CH_PASSWORD
CH_HOST=clickhouse CH_PORT=8123 CH_USER=admin CH_DB=tabyldieva python3 tools/clickhouse_backfill_dm.py
```
**Шаг 7. Проверка:**
```bash
psql -v ds=2026-09-14 -v stg=${S}_stg -v ods=${S}_ods -v dm=${S}_datamart -f sql/90_acceptance_checks.sql
python3 ~/work/testqa-example/project/tools/check_project.py --source-table complaint_cases
```
Пустой стенд (как у одногруппницы) сначала загружает историю: `cfpb_final` (1096 запусков) или нужную дату через `final_tabyldieva_pipeline`; `fdic_financials_pipeline` — кнопкой Trigger.
Затем dbt (`dbt/tabyldieva_dwh/README.md`) и CDC (`cdc/README.md`).

## DAG
| DAG | Расписание | Задачи |
|---|---|---|
| `final_tabyldieva_pipeline` | @daily, catchup выключен; ручной запуск за любую дату | `resolve_ds`; `load_stg_complaints >> reconcile >> load_ods_complaints >> load_ods_derived`; `load_stg_fred >> load_ods_fred`; `load_stg_refs >> load_ods_refs`; затем `dq_ods >> trigger_datamart` |
| `final_tabyldieva_datamart` | запускается основным DAG | `build_dm_complaints`, `build_dm_bank_size` `>> dq_marts >> load_clickhouse` |
| `final_tabyldieva_cdc_integration` | @hourly, создаётся на паузе; включать после CDC | `check_cdc_health >> dbt_build_cdc`: приёмник не пуст (ошибка) и давно ли менялся (предупреждение), затем `dbt build --select tag:cdc`; дополнение |
| `cfpb_final` | исторический backfill 2023–2026 | цепочка для банковского контура (оставлен как история загрузки) |
| `cfpb_bureau_pipeline`, `fdic_financials_pipeline`, `cfpb_resync` | дополнительные | бюро; FDIC раз в месяц; досинхронизация поздних жалоб раз в неделю |

**Какой день грузит запуск** (задача `resolve_ds`). Ручной запуск, в том числе с Logical date, которую выберет преподаватель, грузит ровно эту дату. Запуск по расписанию грузит день на 22 дня раньше своей Logical date: CFPB публикует жалобу после ответа компании или через 15 дней, поэтому за «вчера» API часто отдаёт 0 (проверено 23.10.2026), а день двадцатидвухдневной давности уже устоялся. Число 22 выбрано так, чтобы ночной запуск продолжил окно проекта с 1 октября без дыр. В журнале и витринах стоит загруженный день, а не Logical date запуска. Позднее добавленные жалобы дозагружает `cfpb_resync` раз в неделю.

Каждая задача пишет строку в `tabyldieva_stg.etl_load_log` (источник, дата, начало и конец, статус, число строк, текст ошибки).
Внешний API: таймаут 30 с и 3 повтора на запрос, плюс `retries=1` у задачи; при ошибке в stg ничего не пишется. Адрес API можно подменить
Airflow Variable `tabyldieva_cfpb_base` (так проверяется недоступный адрес); удалить переменную после проверки.

## dbt
`dbt/tabyldieva_dwh/`: структура как у ментора (`models/intermediate` и `marts`, `seeds`, `snapshots`, `tests`, `variants`, `profiles`). Четыре витрины на dbt (две повторяют витрины DAG и сверяются с ними тестами, две новые: по штатам и инкрементальная по типам компаний), слой CDC (`tag:cdc`), два справочника-seed, два снимка (история справочника компаний и история статусов обращений), свои тесты. Запуск и описание: `dbt/tabyldieva_dwh/README.md`.

## CDC (Debezium + Kafka)
Порядок шагов и команды: `cdc/README.md`. Таблица-источник `tabyldieva.complaint_cases` — имитация системы обработки обращений (у открытого API нет потока изменений),
изменения статусов синтетические. Приёмник: `tabyldieva_ods.ods_complaint_cases` (текущее состояние).

## Особенность данных: жалобы появляются с опозданием
CFPB публикует жалобу только после отправки компании, поэтому прошлые дни дополняются задним числом, иногда жалобы снимаются.
Проверено 17.10.2026: в банковском контуре 57 дней расходились с API; после досинхронизации (`cfpb_resync`) расхождений 0, добавилось 854 жалобы.
Свежие ~30 дней всегда недосчитаны: на дашборде их нужно помечать. Ограничение метода досинхронизации: если в 7-дневном блоке одна жалоба
добавилась, а другая снялась, сумма совпадёт и блок пропустится.

## Проверки
```bash
S=$PGUSER     # логин; схемы ${S}_stg, ${S}_ods, ${S}_datamart
# банковский контур: покрытие 1096 дней, нет дыр, stg = ods = datamart
psql -v stg=${S}_stg -v ods=${S}_ods -v dm=${S}_datamart -v d1=2023-10-01 -v d2=2026-09-30 -f sql/11_window_checks.sql
# контур бюро
psql -v stg=${S}_stg -v ods=${S}_ods -v d1=2026-07-01 -v d2=2026-09-30 -f sql/13_bureau_checks.sql
# приёмка за одну дату (слои, витрины, ClickHouse)
psql -v ds=2026-09-14 -v stg=${S}_stg -v ods=${S}_ods -v dm=${S}_datamart -f sql/90_acceptance_checks.sql
# сверка с API по дням (полное окно: тысячи запросов; для показа взять один квартал)
python3 tools/reconcile_api.py --start 2026-07-01 --end 2026-09-30
python3 tools/reconcile_bureau.py
# журнал загрузок
psql -c "select source, status, count(*) from ${S}_stg.etl_load_log group by 1,2 order by 1,2"
```
DAG сами проверяют: `reconcile` (число строк дня = ответ API), `dq_ods` и `dq_marts` (ключи, даты, пустые поля, продукты, суммы витрин),
в `fdic_financials_pipeline` сверку с `meta.total` по каждому кварталу.

## ClickHouse
Копии для аналитики: `tabyldieva.cfpb_complaints` (банки), `tabyldieva.cfpb_bureau` (бюро) и витрина `tabyldieva.dm_complaints_daily`
(`ReplacingMergeTree`, HTTP 8123). Подключение берётся из Airflow Variables `tabyldieva_ch_host`, `_ch_port`, `_ch_user`, `_ch_password`, `_ch_db`;
пароль только там, в файлах его нет. Задачи `load_clickhouse` выполняются, когда `tabyldieva_ch_enabled` = 1, иначе пропускаются.
Таблицы создаются файлами `sql/08_clickhouse.sql`, `sql/14_clickhouse_bureau.sql`, `sql/32_clickhouse_dm.sql` (пароль вводится вручную):
```bash
read -s -p "CH password: " CHP; echo
curl -s --user admin:$CHP "http://clickhouse:8123/" --data-binary @sql/32_clickhouse_dm.sql
curl -s --user admin:$CHP "http://clickhouse:8123/" --data-binary "select count() from tabyldieva.dm_complaints_daily final"
unset CHP
```
Загрузка: удалить день и вставить заново, затем сверить число строк (и `sum(complaints_cnt)` витрины) с Postgres.

## Дополнения сверх минимума
- **Superset на ноутбуке**: витрины выгружаются из ClickHouse в CSV (см. `docs/DEMO.md`); дашборд показывает снимок на дату выгрузки.

## Ограничения (честно)
- Данные американские, сумм жалоб нет.
- Только около 43% жалоб относятся к банкам, застрахованным FDIC (24 компании → 25 банков). Остальное — финтех, бюро, сервисеры, кредитные союзы.
- «Жалоб на 1 млрд активов» — грубый заменитель «жалоб на клиента»; сравнивать осторожно. Слияния банков не проверялись.
- Ставка ФРС показана как контекст дня; причинной связи с числом жалоб проект не утверждает.
- CDC: источник имитирован, статусы меняются скриптом.

## Структура
```
dag/                final_tabyldieva_pipeline.py  final_tabyldieva_datamart.py
ingestion/          cfpb_pipeline.py csv_sources.py datamart_pipeline.py clickhouse_load.py load_cfpb.py cfpb_http.py common.py ...
sql/                01..09 схемы и таблицы · 22..24 FDIC и привязка к банкам · 30 миграция · 31 CSV-таблицы · 32 ClickHouse · 33 история колонок · 90 самопроверка
data/               fred/DFF.csv (ставка ФРС)  ref/dim_company.csv  ref/company_bank_map.csv
dbt/tabyldieva_dwh/ models seeds snapshots tests variants profiles (как у ментора), без пароля
cdc/                sql/ connectors/ scripts/ README.md
docs/               passport.md data_model.md sources.md api_sample.json DEMO.md ...
tools/              backfill_fred.py clickhouse_backfill_dm.py save_api_sample.py reconcile_api.py ...
```
Старые DAG-файлы (`final_cfpb_*.py`, `final_tabyldieva_fdic_financials.py`) лежат рядом с папкой проекта, в `~/work/dags`. Исходники новых лежат в `dag/`, рабочие копии для Airflow кладутся в `~/work/dags` (шаг 4). Копии старых для git — в `dag_batch/` (Airflow эту папку не читает, см. `.airflowignore`).

## Что дальше
Дашборд (последние ~30 дней помечать как неполные), снятие `end_date` у `cfpb_final`, чтобы октябрь проходил по всей цепочке.
