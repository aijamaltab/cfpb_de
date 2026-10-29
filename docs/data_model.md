# Модель данных

PostgreSQL 16, база `aiacademy`, логин `tabyldieva`. Слои stg → ods → datamart, витрины dbt, копии витрин в ClickHouse,
поток изменений CDC. Источники описаны в `docs/passport.md`.

## 1. Слои
| Слой | Схема | Что лежит | Как грузится |
|---|---|---|---|
| stg | `tabyldieva_stg` | данные как пришли: все колонки `text` и служебные `_ds` (день загрузки), `_source_file`, `_loaded_at` | события за день: delete по `_ds` + insert; справочники целиком |
| ods | `tabyldieva_ods` | типы (`date`, `timestamp`, `numeric`, `boolean`), понятные имена, первичный ключ | delete + insert за день из stg; справочники целиком |
| datamart | `tabyldieva_datamart` | витрины и таблица метрик качества `dq_metrics` | delete + insert за день, месяц или целиком |
| dbt | `tabyldieva_dbt`, `tabyldieva_dbt_snapshots` | модели dbt, seed-справочники, снимки истории | `dbt build` |
| «база сайта» | `tabyldieva` | таблица `complaint_cases`, источник CDC | её меняет «приложение», Debezium читает журнал |
| журнал | `tabyldieva_stg.etl_load_log` | строка на каждый шаг каждого запуска | пишет код DAG |

`etl_load_log`: `id` (PK), `source` (шаг), `ds`, `status` (`running` / `success` / `failed`), `started_at`, `finished_at`,
`rows_loaded`, `error`. Для скрипта проверки ментора есть представление `tabyldieva_meta.load_log` над этой же таблицей.

Правило слоёв: stg ничего не чистит, ods не теряет и не добавляет жалоб, витрины только агрегируют ods.

## 2. Таблицы stg и ods
| stg | ods | Одна строка ods | Ключ | Источник |
|---|---|---|---|---|
| `stg_cfpb_complaints` | `ods_complaints` | жалоба | `complaint_id` | API CFPB |
| `stg_fdic_financials` | `ods_fdic_financials` | банк × квартал | `(cert, report_date)` | API FDIC |
| `stg_fred_dff` | `ods_fred_dff` | календарный день | `ds` | CSV FRED |
| `stg_dim_company` | `ods_dim_company` | компания | `company_cfpb` | CSV |
| `stg_company_bank_map` | `ods_company_bank` | пара компания — банк | `(company_cfpb, cert)` | CSV |
| — (пишет sink-коннектор) | `ods_complaint_cases` | обращение, текущее состояние | `complaint_id` | CDC |
| — (из `ods_complaints`) | `ods_products`, `ods_companies` | продукт, компания | `(product, sub_product)`, `company` | производные |

Типы в `ods_complaints`: `received_at` и `sent_to_company_at` — `timestamp` (UTC), `ds` — `date` (день получения), `timely` —
`boolean`, `zip_code` — `text` (чтобы не терять ведущие нули). Индекс по `ds`.
`ods_fdic_financials`: `assets_k_usd`, `deposits_k_usd`, `net_income_k_usd`, `equity_k_usd` — `bigint`, тысячи USD; `roa`, `roe` — `numeric`.
`ods_fred_dff`: `fed_funds_rate numeric` (% годовых), `is_carried boolean` (ставку за день ещё не опубликовали, перенесена последняя).

Жалобы связаны с банками так: `ods_complaints.company` = `ods_company_bank.company_cfpb`, затем `cert` = `ods_fdic_financials.cert`.
Сопоставление покрывает около 43% жалоб.

## 3. Витрины DAG (`tabyldieva_datamart`)

### dm_complaints_daily — день × продукт
Ключ `(ds, product)`. Витрина по времени.
| Колонка | Что значит |
|---|---|
| `complaints_cnt` | жалоб за день по продукту |
| `timely_cnt` | из них отвечено в срок (`timely = true`) |
| `untimely_cnt` | из них с опозданием (`timely = false`) |
| `in_progress_cnt` | из них ещё в работе (`company_response = 'In progress'`) |
| `relief_cnt` | из них закрыто с компенсацией (`Closed with monetary relief` или `Closed with non-monetary relief`) |
| `fed_funds_rate` | ставка ФРС в этот день, % годовых (CSV FRED) |

Проверка: сумма `complaints_cnt` за день равна числу строк `ods_complaints` за этот день, иначе DAG падает.

### dm_company_month — месяц × компания
Ключ `(month, company)`. Витрина по справочнику.
| Колонка | Что значит |
|---|---|
| `month` | первое число месяца |
| `company_type` | тип из `ods_dim_company`; `other`, если компании нет в справочнике |
| `complaints_cnt` | жалоб за месяц |
| `untimely_cnt` | из них с опозданием |
| `relief_cnt` | из них с компенсацией |

Проверка: сумма за месяц равна сумме `dm_complaints_daily` за дни этого месяца.

### dm_bank_size_quarter — квартал × компания (метрика на данных API FDIC)
Ключ `(quarter_start, company)`. В витрину попадают только кварталы, которые целиком лежат в окне жалоб и уже опубликованы FDIC.
| Колонка | Что значит |
|---|---|
| `banks_cnt` | сколько банков FDIC у компании |
| `complaints_cnt` | жалоб на компанию за квартал |
| `assets_k_usd`, `deposits_k_usd` | активы и депозиты её банков на конец квартала, тыс. USD |
| `complaints_per_bn_assets` | жалоб на 1 млрд USD активов: `complaints_cnt / (assets_k_usd / 1e6)` |
| `complaints_per_bn_deposits` | то же на 1 млрд USD депозитов |

Сейчас 264 строки: 24 компании × 11 кварталов. Если на дашборде выбран период из нескольких кварталов, метрика считается как
сумма жалоб на сумму активов, то есть средняя за квартал с весом по размеру банка.

## 4. Модели dbt (`tabyldieva_dbt`)
- `models/intermediate` (view): `int_complaints_enriched` — жалоба с типом компании, названием штата и флагами «в работе» и
  «компенсация»; `int_cases_current` — обращение CDC в текущем состоянии.
- `models/marts` (table): `dim_calendar`; `dm_complaints_daily` и `dm_company_month` — те же витрины, что в DAG, тесты
  `assert_dbt_matches_dag_daily` и `assert_dbt_matches_dag_month` сравнивают их построчно; `dm_state_month` (месяц × штат);
  `dm_type_daily` (день × тип компании, инкрементальная: пересчитывает последние дни по переменной `lookback_days`, полная
  пересборка через `--full-refresh`).
- Слой CDC (тег `cdc`): `dm_open_cases` (продукт × статус: сколько обращений и их средний возраст в днях), `audit_cdc_vs_batch`
  (где поток и батч разошлись).
- Seeds: `us_states`, `complaint_status`. Снимки: `dim_company_snapshot` (история справочника компаний),
  `complaint_cases_snapshot` (история статусов обращений).
- Тесты: `unique`, `not_null`, `accepted_values`, `relationships` и свои. `assert_marts_complaints_equal` проверяет, что число
  жалоб в ods равно сумме по каждой витрине.

## 5. ClickHouse (база `tabyldieva`)
Шесть витрин копируются под теми же именами и с теми же колонками, движок `ReplacingMergeTree`. Superset читает их отсюда.
| Таблица | Откуда | Кто обновляет | Что перезаписывается |
|---|---|---|---|
| `dm_complaints_daily` | `tabyldieva_datamart` | DAG витрин, задача `load_clickhouse` | день `ds` |
| `dm_company_month` | `tabyldieva_datamart` | DAG витрин | месяц дня `ds` |
| `dm_bank_size_quarter` | `tabyldieva_datamart` | DAG витрин | вся таблица |
| `dm_state_month`, `dm_type_daily` | `tabyldieva_dbt` | `final_tabyldieva_dbt_marts`, раз в сутки | вся таблица |
| `dm_open_cases` | `tabyldieva_dbt` | `final_tabyldieva_cdc_integration`, раз в час | вся таблица |
| `dq_metrics` | `tabyldieva_datamart` | `final_tabyldieva_dq`, раз в сутки | вся таблица |

Каждая загрузка удаляет срез, вставляет его заново и сверяет с Postgres число строк и суммы метрик. Если не сошлось, задача
красная, в журнале строка `failed` с текстом.

## 6. CDC: `tabyldieva.complaint_cases`
`complaint_id` (PK, номер жалобы CFPB), `product`, `company`, `state`, `status` (`in_progress`, `closed_explanation`,
`closed_relief`, `closed_other`), `timely`, `received_at`, `updated_at` (меняется при каждом UPDATE).
Приёмник `tabyldieva_ods.ods_complaint_cases` хранит текущее состояние, одна строка на обращение. Изменения синтетические.

## 7. Метрики качества: `tabyldieva_datamart.dq_metrics`
Одна строка = одна метрика за день проверки. Ключ `(check_date, table_name, metric)`.
Колонки: `dimension` (полнота, уникальность, валидность, согласованность, точность, актуальность), `layer`, `table_name`,
`metric` (например `null_rate(company)`), `title`, `value`, `threshold`, `unit`, `severity` (`error` или `warn`), `status`
(`ok`, `warn`, `fail`), `failed`.
| Измерение | Формула | Пример |
|---|---|---|
| полнота | `count(*) filter (where col is null) / count(*)` | жалобы без компании; дни без жалоб |
| уникальность | `1 - count(distinct ключ) / count(*)` | повтор жалобы после ретрая |
| валидность | доля значений вне справочника или формата | продукт вне списка, неизвестный ответ компании |
| согласованность | число строк, дней или месяцев, где части не сходятся с итогом | ods по дням против витрины |
| точность | `abs(приёмник - источник) / источник`, проверка единиц | ClickHouse против Postgres; ставка в долях вместо % |
| актуальность | `now() - max(время загрузки)` | часы с последней сборки витрины |

## 8. Вопросы бизнеса одним запросом к витрине
1. Сколько жалоб по каждому продукту за месяц:
   ```sql
   select product, sum(complaints_cnt) from tabyldieva_datamart.dm_complaints_daily
   where ds between '2026-08-01' and '2026-08-31' group by 1 order by 2 desc;
   ```
2. Топ компаний за месяц и доля ответов с опозданием:
   ```sql
   select company, complaints_cnt, round(100.0 * untimely_cnt / complaints_cnt, 1) as pct_untimely
   from tabyldieva_datamart.dm_company_month where month = '2026-08-01' order by complaints_cnt desc limit 10;
   ```
3. Как быстро отвечают и чем закрывают, по месяцам:
   ```sql
   select date_trunc('month', ds)::date as month, sum(complaints_cnt) as complaints,
          round(100.0 * sum(timely_cnt) / sum(complaints_cnt), 1) as pct_timely,
          round(100.0 * sum(relief_cnt) / sum(complaints_cnt), 1) as pct_relief,
          round(100.0 * sum(in_progress_cnt) / sum(complaints_cnt), 1) as pct_in_progress
   from tabyldieva_datamart.dm_complaints_daily group by 1 order by 1;
   ```
4. У каких банков жалоб больше при том же размере:
   ```sql
   select company, complaints_cnt, round(assets_k_usd / 1e6) as assets_bn_usd, complaints_per_bn_assets
   from tabyldieva_datamart.dm_bank_size_quarter where quarter_start = '2026-04-01'
   order by complaints_per_bn_assets desc limit 10;
   ```
5. Сколько обращений в работе и как давно они открыты:
   ```sql
   select product, cases_cnt, avg_age_days from tabyldieva_dbt.dm_open_cases
   where status_group = 'open' order by cases_cnt desc;
   ```
Сверка двух витрин за один период:
```sql
select (select sum(complaints_cnt) from tabyldieva_datamart.dm_complaints_daily
        where ds >= '2026-08-01' and ds < '2026-09-01') as daily,
       (select sum(complaints_cnt) from tabyldieva_datamart.dm_company_month where month = '2026-08-01') as monthly;
```
