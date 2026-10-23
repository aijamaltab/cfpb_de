# Модель данных

Хранилище: PostgreSQL 16, база `aiacademy`, логин `tabyldieva`. Слои: `stg` → `ods` → `datamart`, копии витрин в ClickHouse, поток изменений CDC.
Источники и их роли: `docs/passport.md`. Контракты колонок и проверки: `docs/sources.md`.

## 1. Слои

| Слой | Схема | Что хранит | Режим загрузки |
|---|---|---|---|
| stg | `tabyldieva_stg` | данные как пришли: все колонки `text`, плюс `_ds` (день загрузки), `_source_file`, `_loaded_at` | события и API за день: delete по `_ds` + insert; справочники: целиком (truncate + insert) |
| ods | `tabyldieva_ods` | очищенные данные: типы (`date`, `timestamp`, `numeric`, `boolean`), понятные имена, первичный ключ | delete + insert за день из stg (справочники целиком) |
| datamart | `tabyldieva_datamart` | готовые витрины для запросов и дашборда | delete + insert за день, месяц или квартал |
| «база сайта» | `tabyldieva` | таблица-источник CDC `complaint_cases` | её меняют снаружи; Debezium читает журнал |
| dbt | `tabyldieva_dbt`, `tabyldieva_dbt_snapshots` | dbt-версии витрин, справочники-seed, снимки (история), проверяются тестами | `dbt build` |
| журнал | `tabyldieva_stg.etl_load_log` | строка на каждый шаг каждого запуска; `tabyldieva_meta.load_log` — view-отражение для проверки ментора | пишет код DAG |

Журнал `etl_load_log`: `id` (PK), `source` (имя шага), `ds`, `status` (`running` / `success` / `failed`), `started_at`, `finished_at`, `rows_loaded`, `error`.

## 2. Таблицы stg и ods

| stg | ods | Зерно ods (одна строка =) | Ключ | Источник |
|---|---|---|---|---|
| `stg_cfpb_complaints` | `ods_complaints` | одна жалоба | `complaint_id` | API CFPB |
| `stg_fred_dff` | `ods_fred_dff` | одна календарная дата | `ds` | CSV FRED DFF |
| `stg_dim_company` | `ods_dim_company` | одна компания | `company_cfpb` | CSV справочник |
| `stg_company_bank_map` | `ods_company_bank` | пара «компания — банк FDIC» | `(company_cfpb, cert)` | CSV справочник |
| `stg_fdic_financials` | `ods_fdic_financials` | банк × квартал | `(cert, report_date)` | API FDIC |
| — (пишет Debezium) | `ods_complaint_cases` | одно обращение, текущее состояние | `complaint_id` | CDC `tabyldieva.complaint_cases` |
| — (из `ods_complaints`) | `ods_products`, `ods_companies` | продукт (подпродукт), компания: справочники, пересобираются после каждого дня | `(product, sub_product)`, `company` | производные |

Ключевые типы в `ods_complaints`: `received_at timestamp` (время получения по UTC), `ds date` (день получения), `timely boolean`, остальные поля `text`.
`ods_fdic_financials`: суммы в тысячах USD (`assets_k_usd`, `deposits_k_usd`, `net_income_k_usd`, `equity_k_usd` — `bigint`), `roa`, `roe` — `numeric`.
`ods_fred_dff.fed_funds_rate numeric` (% годовых); `ods_fred_dff.is_carried boolean`: `true`, если за день FRED ещё не опубликовал значение и перенесена последняя известная ставка.

## 3. Витрины

### dm_complaints_daily (зерно: день × продукт)
Ключ `(ds, product)`. Витрина «по времени».

| Колонка | Определение |
|---|---|
| `complaints_cnt` | число жалоб за день по продукту (`count(*)` из `ods_complaints`) |
| `timely_cnt` | из них отвечено в срок (`timely = true`) |
| `untimely_cnt` | из них с опозданием (`timely = false`) |
| `in_progress_cnt` | из них `company_response = 'In progress'` |
| `relief_cnt` | из них закрыто с денежной или неденежной компенсацией |
| `fed_funds_rate` | ставка ФРС на этот день, % годовых (CSV FRED DFF; единая для всех продуктов дня) |

Правило: `complaints_cnt` = число строк `ods_complaints` за этот день по продукту; DAG падает, если сумма за день не сошлась.

### dm_company_month (зерно: месяц × компания)
Ключ `(month, company)`. Витрина «по справочнику»: тип компании берётся из CSV-справочника.

| Колонка | Определение |
|---|---|
| `month` | первое число месяца |
| `company_type` | тип из `ods_dim_company`; `other`, если компании нет в справочнике |
| `complaints_cnt` | жалоб за месяц |
| `untimely_cnt` | из них отвечено с опозданием |
| `relief_cnt` | из них с компенсацией |

Сверка двух витрин: `sum(complaints_cnt)` за календарный месяц в `dm_company_month` равна `sum(complaints_cnt)` за те же дни в `dm_complaints_daily`.

### dm_bank_size_quarter (зерно: квартал × компания; использует данные API FDIC)
Ключ `(quarter_start, company)`. Только кварталы, целиком лежащие в загруженном окне жалоб и уже опубликованные FDIC.

| Колонка | Определение |
|---|---|
| `banks_cnt` | сколько банков FDIC учтено у компании (по `ods_company_bank`) |
| `complaints_cnt` | жалоб на компанию за квартал |
| `assets_k_usd`, `deposits_k_usd` | сумма активов и депозитов банков компании на конец квартала, тыс. USD |
| `complaints_per_bn_assets` | жалоб на 1 млрд USD активов (`complaints_cnt / (assets_k_usd / 1e6)`) |
| `complaints_per_bn_deposits` | то же на 1 млрд USD депозитов |

Метрика грубая: компании без банков в FDIC (финтех, бюро) в витрину не попадают (покрыто около 43% жалоб).

### Витрины в ClickHouse
Все шесть витрин копируются в базу `tabyldieva` ClickHouse под теми же именами и с теми же колонками (движок ReplacingMergeTree; служебные `_dbt_run_id` и `_built_at` не копируются). Дашборды читают их оттуда. Требование ФТТ (одна витрина) выполнено, остальные сверх минимума.

| Таблица | Источник в Postgres | Кто обновляет | Срез при перезагрузке |
|---|---|---|---|
| `dm_complaints_daily` | `<логин>_datamart` | задача `load_clickhouse` | день `ds` |
| `dm_company_month` | `<логин>_datamart` | задача `load_clickhouse_company_month` | месяц дня `ds` |
| `dm_bank_size_quarter` | `<логин>_datamart` | задача `load_clickhouse_bank_size` | вся таблица (в Postgres тоже пересборка целиком) |
| `dm_state_month` | `<логин>_dbt` | DAG `final_tabyldieva_dbt_marts` (раз в сутки, после `dbt build`) | вся таблица |
| `dm_type_daily` | `<логин>_dbt` | тот же DAG | вся таблица |
| `dm_open_cases` | `<логин>_dbt` (слой CDC) | DAG `final_tabyldieva_cdc_integration` (раз в час, после `dbt build -s tag:cdc`) | вся таблица (снимок «на сейчас») |

Каждая задача и скрипт удаляют срез, вставляют заново и сверяют с Postgres число строк и суммы метрик (`complaints_cnt` и другие целые метрики витрины); при несовпадении задача красная, в `etl_load_log` строка `failed` с текстом. Витрины dbt обновляются отдельными DAG (оба на паузе, пока их не включат) или вручную скриптом `tools/clickhouse_backfill_marts.py`. Если DAG выключен, а `dbt build` прошёл, в ClickHouse остаётся прошлая сборка.

### dbt-модели (схема `tabyldieva_dbt`)
Структура папок как у ментора. `models/intermediate` (view): `int_complaints_enriched` (жалоба + тип компании + название штата + флаги «в работе» и «компенсация»), `int_cases_current` (CDC: обращение «как сейчас», батч и поток).
`models/marts` (table): `dim_calendar`; `dm_complaints_daily` и `dm_company_month` повторяют витрины DAG (тесты `assert_dbt_matches_dag_daily` / `_month` сравнивают их с витринами DAG построчно); `dm_state_month` (месяц × штат, названия из seed `us_states`); `dm_type_daily` (день × тип компании, **инкрементальная**: пересчитывает последние 30 дней, `dbt build --full-refresh -s dm_type_daily` для полной пересборки).
CDC (метка `cdc`): `dm_open_cases` (зерно: продукт × статус) считает обращения «в работе» и средний возраст по `ods_complaint_cases`; `audit_cdc_vs_batch` показывает, где батч и поток разошлись.
Seeds: `us_states`, `complaint_status`. Снимки (схема `tabyldieva_dbt_snapshots`): `dim_company_snapshot` (история справочника), `complaint_cases_snapshot` (история статусов обращений). Инвариант `assert_marts_complaints_equal`: число жалоб в ods равно сумме по каждой витрине.

## 4. Таблица CDC: tabyldieva.complaint_cases
`complaint_id` (PK, номер жалобы CFPB), `product`, `company`, `state`, `status` (`in_progress`, `closed_explanation`, `closed_relief`, `closed_other`), `timely`, `received_at`, `updated_at` (меняется при любом UPDATE). Приёмник: `tabyldieva_ods.ods_complaint_cases` — текущее состояние, одна строка на обращение.
Данные об изменениях синтетические (см. `docs/passport.md`, раздел 6).

## 5. Бизнес-вопросы и запросы одной строкой к витрине (без JOIN)

1. **Сколько жалоб по каждому продукту за период?**
   ```sql
   select product, sum(complaints_cnt) from tabyldieva_datamart.dm_complaints_daily
   where ds between '2026-08-01' and '2026-08-31' group by 1 order by 2 desc;
   ```
2. **Какие компании в топе по жалобам за месяц и какая доля просрочек?**
   ```sql
   select company, complaints_cnt, round(100.0 * untimely_cnt / complaints_cnt, 1) as pct_untimely
   from tabyldieva_datamart.dm_company_month where month = '2026-08-01' order by complaints_cnt desc limit 10;
   ```
3. **Как отличаются просрочки по типам компаний?**
   ```sql
   select company_type, sum(complaints_cnt), round(100.0 * sum(untimely_cnt) / sum(complaints_cnt), 1) as pct_untimely
   from tabyldieva_datamart.dm_company_month where month = '2026-08-01' group by 1 order by 2 desc;
   ```
4. **У каких банков жалоб больше при том же размере?**
   ```sql
   select company, complaints_cnt, round(assets_k_usd / 1e6) as assets_bn_usd, complaints_per_bn_assets
   from tabyldieva_datamart.dm_bank_size_quarter
   where quarter_start = (select max(quarter_start) from tabyldieva_datamart.dm_bank_size_quarter)
   order by complaints_per_bn_assets desc limit 10;
   ```
5. **Сколько обращений «в работе» и как давно они открыты?** (по CDC)
   ```sql
   select status, count(*), round(avg(extract(epoch from now() - received_at) / 86400)::numeric, 1) as avg_age_days
   from tabyldieva_ods.ods_complaint_cases group by 1 order by 2 desc;
   ```
6. **Видна ли связь числа жалоб со ставкой ФРС?** (только описательно, причинность не утверждается)
   ```sql
   select date_trunc('month', ds)::date as month, sum(complaints_cnt) as complaints, round(avg(fed_funds_rate), 2) as avg_rate
   from tabyldieva_datamart.dm_complaints_daily group by 1 order by 1;
   ```

## 6. Проверки качества
| Где | Что проверяется |
|---|---|
| DAG, `dq_ods` | пустые company/product; дата получения = день `ds`; ответ компании не раньше получения; жалоба не лежит в двух днях; продукт вне списка банковских; объём дня не меньше 10% среднего (для дат младше 14 дней не проверяется: жалобы публикуются с опозданием). Дубли `complaint_id` невозможны: первичный ключ ods. Справочники при загрузке: допустимые `company_type`, компания из привязки к банку есть в справочнике |
| DAG, `dq_marts` | 7 проверок витрин: суммы по дню, месяцу и продуктам сходятся с ods и между собой; ставка ФРС заполнена; нет отрицательных значений |
| dbt | `unique`, `not_null`, `accepted_values` (статусы и типы компаний), `non_negative`, сравнение с витринами DAG |
| `sql/90_acceptance_checks.sql` | самопроверка по критериям приёмки ФТТ |
