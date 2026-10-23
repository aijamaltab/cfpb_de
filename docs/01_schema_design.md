# День 2. Схема данных и слои хранилища

## Слои (названия как в курсе)
| Слой | Схема | Что лежит | Режим загрузки |
|---|---|---|---|
| stg | `<login>_stg` | всё как пришло, все колонки text + `_ds`, `_source_file`, `_loaded_at` | delete + insert по `_ds` (FDIC: truncate + reload) |
| ods | `<login>_ods` | типы, ключ, понятные имена, `ds` | delete + insert по `ds` (FDIC и справочники: пересборка целиком) |
| ods (справочники) | `<login>_ods` | `ods_products`, `ods_companies` (из жалоб), `ods_dim_company`, `ods_company_bank` (из CSV) | truncate + insert |
| dm | `<login>_datamart` | витрины для дашборда | delete + insert за день / месяц; `dm_bank_size_quarter` пересобирается целиком |
| служебное | `<login>_stg.etl_load_log` | журнал: source, ds, status, started_at, finished_at, rows_loaded, error | append |

Правило: stg ничего не чистит и не вычисляет; ods не теряет жалобу и не добавляет данных; dm только агрегирует ods.

## Таблицы
**Жалобы CFPB (два контура с одной структурой)**
- `stg_cfpb_complaints` / `stg_cfpb_bureau` — 15 полей API (text) + служебные. Ключа нет: в stg допустимы повторы, ods снимает дубли.
- `ods_complaints` / `ods_bureau` — PK `complaint_id` (bigint); `received_at`, `sent_to_company_at` (timestamp, UTC); `product`, `company` not null;
  `zip_code` text (ведущие нули); `timely` boolean (Yes/No → true/false, пусто → null); `ds` date, индекс по `ds`.
  Банки: 855 110 строки; бюро: 1 909 762 строки; пересечения по `complaint_id` нет.

**Банки FDIC**
- `stg_fdic_financials` — CERT, REPDTE, ASSET, DEP, NETINC, ROA, ROE, EQ, NUMEMP (text) + служебные.
- `ods_fdic_financials` — PK `(cert, report_date)`; `assets_k_usd`, `deposits_k_usd`, `net_income_k_usd`, `equity_k_usd` в тысячах USD, `roa`, `roe`, `employees`.
  14 кварталов, около 63,8 тыс. строк.
- `ods_company_bank` — PK `(company_cfpb, cert)`: компания как в жалобах → банк FDIC. Из `data/ref/company_bank_map.csv` (24 компании, 25 банков).

**Справочники ods**: `ods_products` (PK product + sub_product), `ods_companies` (PK company): первый и последний день, число жалоб.

**CSV-источники** (`sql/31_csv_tables.sql`)
- `stg_fred_dff` → `ods_fred_dff` — PK `ds`; ставка ФРС по дням, `fed_funds_rate numeric`, `is_carried boolean` (true: за день FRED ещё не опубликовал значение, перенесена последняя известная ставка). Файл `data/fred/DFF.csv`.
- `stg_dim_company` → `ods_dim_company` — PK `company_cfpb`; 60 компаний, тип компании. Файл `data/ref/dim_company.csv`.
- `stg_company_bank_map` → `ods_company_bank` (см. выше). Файл `data/ref/company_bank_map.csv`.

**Витрины dm**
- `dm_complaints_daily` (PK ds + product): жалоб, в срок, не в срок, в работе, с компенсацией, `fed_funds_rate` (ставка ФРС дня, CSV).
- `dm_company_month` (PK month + company): жалоб, просрочек, компенсаций, `company_type` (из CSV-справочника; `other`, если компании там нет).
- `dm_bank_size_quarter` (PK quarter_start + company): жалоб, активы, депозиты, жалоб на 1 млрд USD активов и депозитов. Только кварталы целиком внутри окна жалоб.
  Пересобирается `sql/24_dm_bank_size.sql` (после каждой досинхронизации запускать заново).

**CDC**: источник `tabyldieva.complaint_cases` (схема «базы сайта», PK `complaint_id`, статус, `updated_at`), приёмник `tabyldieva_ods.ods_complaint_cases` (текущее состояние, создаёт sink-коннектор). Файлы и порядок: `cdc/README.md`. Данные об изменениях синтетические.

**ClickHouse**: `tabyldieva.dm_complaints_daily` (`sql/32_clickhouse_dm.sql`, витрина DAG; это требование ФТТ) и прежние копии ods `cfpb_complaints`, `cfpb_bureau` (`sql/08_clickhouse.sql`, `sql/14_clickhouse_bureau.sql`); всё `ReplacingMergeTree`.

Полное описание зерна, ключей и метрик для приёмки: `docs/data_model.md`.

## Словарь ключевых полей
| Поле | Смысл |
|---|---|
| complaint_id | уникальный номер жалобы в CFPB |
| date_received | день подачи (UTC; по нему режем загрузку) |
| product / sub_product | вид финансового продукта |
| company | компания, на которую жалоба |
| company_response | статус: In progress, Closed with explanation, Closed with monetary relief, Closed with non-monetary relief |
| timely | ответила ли компания в срок |
| cert | номер банка в FDIC |
| report_date | конец квартала, на который даны показатели банка |
| assets_k_usd | активы банка, тыс. USD |

## Связи и ключи
Один ключ `complaint_id` сквозь все слои жалоб. Жалобы связаны с банками через `ods_company_bank` (`company` = `company_cfpb`, затем `cert` = `cert` в `ods_fdic_financials`).
Справочники строятся из ods, внешних ключей между слоями нет (аналитическое хранилище). Витрины связаны с ods по `ds`, `product`, `company`.
Покрытие сопоставления: около 43% жалоб банковского контура.
