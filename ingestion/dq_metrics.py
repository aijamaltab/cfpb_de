"""Метрики качества данных по шести измерениям: полнота, уникальность, валидность, согласованность, точность, актуальность.

Зачем: проверки в DAG (dq_check, dq_marts, тесты dbt) отвечают «да / нет» и останавливают конвейер. Здесь те же вопросы
считаются числом (доля, расхождение, отставание) и копятся по дням в таблице <логин>_datamart.dq_metrics, поэтому видно,
что качество ухудшается, раньше, чем что-то упадёт. Копия таблицы в ClickHouse - вкладка «Качество данных» в Superset.

Формулы (на каждую метрику - один SQL, возвращающий одно число):
  полнота         null_rate = count(*) filter (where col is null) / count(*)
  уникальность    dup_rate  = 1 - count(distinct ключ) / count(*)
  валидность      bad_rate  = count(*) filter (where значение вне домена или формата) / count(*)
  согласованность mismatch  = число строк / дней / месяцев, где части не сходятся с итогом
  точность        drift     = |сумма в приёмнике - сумма в источнике| / сумма в источнике; out_of_range - единицы измерения
  актуальность    lag       = now() - max(время события или успешной загрузки)
Статус: значение <= порога -> ok; иначе fail (severity = error, DAG красный) или warn (только сигнал).
Перезапуск за ту же дату безопасен: delete + insert по check_date.
"""
from datetime import date, datetime

import cfpb_pipeline as cp
import load_cfpb
from common import SCHEDULE_LAG_DAYS          # основной DAG грузит день logical date - 22: это нормальное отставание

STG, ODS, DM = cp.STG, cp.ODS, cp.DM
DBT = cp.LOGIN + "_dbt"
TABLE = DM + ".dq_metrics"

DDL = """
create table if not exists """ + TABLE + """ (
    check_date  date        not null,          -- день проверки (logical date запуска)
    dimension   text        not null,          -- полнота / уникальность / валидность / согласованность / точность / актуальность
    layer       text        not null,          -- stg / ods / datamart / dbt / clickhouse / cdc
    table_name  text        not null,
    metric      text        not null,          -- например null_rate(company)
    title       text        not null,          -- что проверяем, по-русски
    value       numeric,                       -- значение метрики (null: не удалось посчитать)
    threshold   numeric     not null,          -- порог: ok, если value <= threshold
    unit        text        not null,          -- доля / строк / дней / часов
    severity    text        not null,          -- error: красный DAG; warn: только сигнал
    status      text        not null,          -- ok / warn / fail
    failed      int         not null,          -- 1, если status = fail (для суммы и сверки с ClickHouse)
    checked_at  timestamptz not null default now(),
    primary key (check_date, table_name, metric)
)"""

RESPONSES = ["Closed with explanation", "Closed with non-monetary relief", "Closed with monetary relief",
             "In progress", "Closed", "Untimely response"]
FRESH_LOG_SQL = ("select extract(epoch from now() - max(finished_at)) / 3600 from " + STG + ".etl_load_log "
                 "where source = %(src)s and status = 'success'")


def _null_rate(table, col):
    return f"select count(*) filter (where {col} is null)::numeric / nullif(count(*), 0) from {table}"


def _dup_rate(table, key):
    return f"select 1 - count(distinct ({key}))::numeric / nullif(count(*), 0) from {table}"


# (измерение, слой, таблица, метрика, описание, SQL -> одно число, порог, единица, severity)
PG_METRICS = [
    # ---- полнота: все ли данные доехали
    ("полнота", "ods", "ods_complaints", "null_rate(company)", "Доля жалоб без компании",
     _null_rate(ODS + ".ods_complaints", "nullif(company, '')"), 0, "доля", "error"),
    ("полнота", "ods", "ods_complaints", "null_rate(product)", "Доля жалоб без продукта",
     _null_rate(ODS + ".ods_complaints", "nullif(product, '')"), 0, "доля", "error"),
    ("полнота", "ods", "ods_complaints", "null_rate(company_response)", "Доля жалоб без ответа компании",
     _null_rate(ODS + ".ods_complaints", "company_response"), 0.01, "доля", "warn"),
    ("полнота", "ods", "ods_complaints", "null_rate(timely)", "Доля жалоб без признака «вовремя»",
     _null_rate(ODS + ".ods_complaints", "timely"), 0.01, "доля", "warn"),
    ("полнота", "ods", "ods_complaints", "null_rate(state)", "Доля жалоб без штата (CFPB допускает пустой)",
     _null_rate(ODS + ".ods_complaints", "state"), 0.02, "доля", "warn"),
    ("полнота", "ods", "ods_complaints", "missing_days", "Дней без единой жалобы внутри периода загрузки",
     "select count(*) from generate_series((select min(ds) from " + ODS + ".ods_complaints), "
     "(select max(ds) from " + ODS + ".ods_complaints), interval '1 day') d "
     "where not exists (select 1 from " + ODS + ".ods_complaints o where o.ds = d::date)", 0, "дней", "error"),
    ("полнота", "datamart", "dm_complaints_daily", "null_rate(fed_funds_rate)", "Доля строк витрины без ставки ФРС",
     _null_rate(DM + ".dm_complaints_daily", "fed_funds_rate"), 0, "доля", "warn"),
    # ---- уникальность: нет ли дублей одной сущности
    ("уникальность", "stg", "stg_cfpb_complaints", "dup_rate(complaint_id, _ds)", "Дубли жалобы внутри дня загрузки (ретраи)",
     _dup_rate(STG + ".stg_cfpb_complaints", "complaint_id, _ds"), 0, "доля", "error"),
    ("уникальность", "stg", "stg_cfpb_complaints", "dup_rate(complaint_id)", "Жалоба загружена в разные дни",
     _dup_rate(STG + ".stg_cfpb_complaints", "complaint_id"), 0, "доля", "error"),
    ("уникальность", "ods", "ods_complaints", "dup_rate(complaint_id)", "Дубли жалобы в ods",
     _dup_rate(ODS + ".ods_complaints", "complaint_id"), 0, "доля", "error"),
    ("уникальность", "datamart", "dm_complaints_daily", "dup_rate(ds, product)", "Дубли строки день x продукт",
     _dup_rate(DM + ".dm_complaints_daily", "ds, product"), 0, "доля", "error"),
    ("уникальность", "datamart", "dm_company_month", "dup_rate(month, company)", "Дубли строки месяц x компания",
     _dup_rate(DM + ".dm_company_month", "month, company"), 0, "доля", "error"),
    # ---- валидность: соответствует ли значение формату и домену
    ("валидность", "ods", "ods_complaints", "bad_rate(product)", "Продукт вне списка банковских",
     "select count(*) filter (where product <> all(%(products)s))::numeric / nullif(count(*), 0) from "
     + ODS + ".ods_complaints", 0, "доля", "error"),
    ("валидность", "ods", "ods_complaints", "bad_rate(company_response)", "Ответ компании вне справочника CFPB",
     "select count(*) filter (where company_response is not null and company_response <> all(%(responses)s))::numeric "
     "/ nullif(count(*), 0) from " + ODS + ".ods_complaints", 0, "доля", "error"),
    ("валидность", "ods", "ods_complaints", "bad_rate(received_at = ds)", "Дата жалобы не совпадает с днём загрузки",
     "select count(*) filter (where received_at::date <> ds)::numeric / nullif(count(*), 0) from " + ODS + ".ods_complaints",
     0, "доля", "error"),
    ("валидность", "ods", "ods_complaints", "bad_rate(sent >= received)", "Передана компании раньше, чем поступила",
     "select count(*) filter (where sent_to_company_at < received_at - interval '1 day')::numeric / nullif(count(*), 0) from "
     + ODS + ".ods_complaints", 0, "доля", "error"),
    # ---- согласованность: не противоречат ли данные друг другу
    ("согласованность", "datamart", "dm_complaints_daily", "mismatch(части > итога)",
     "Строк, где «вовремя + с опозданием» или «компенсация + в работе» больше всех жалоб",
     "select count(*) from " + DM + ".dm_complaints_daily where timely_cnt + untimely_cnt > complaints_cnt "
     "or relief_cnt + in_progress_cnt > complaints_cnt", 0, "строк", "error"),
    ("согласованность", "datamart", "dm_complaints_daily", "mismatch(ods по дням)", "Дней, где сумма витрины не равна числу жалоб в ods",
     "select count(*) from (select ds, count(*) n from " + ODS + ".ods_complaints group by ds) o "
     "full join (select ds, sum(complaints_cnt) n from " + DM + ".dm_complaints_daily group by ds) d using (ds) "
     "where coalesce(o.n, 0) <> coalesce(d.n, 0)", 0, "дней", "error"),
    ("согласованность", "datamart", "dm_company_month", "mismatch(день и месяц)", "Месяцев, где витрина по компаниям не равна витрине по дням",
     "select count(*) from (select date_trunc('month', ds)::date m, sum(complaints_cnt) n from " + DM + ".dm_complaints_daily group by 1) d "
     "full join (select month m, sum(complaints_cnt) n from " + DM + ".dm_company_month group by 1) c using (m) "
     "where coalesce(d.n, 0) <> coalesce(c.n, 0)", 0, "месяцев", "error"),
    ("согласованность", "dbt", "dm_open_cases", "mismatch(CDC)", "Расхождение витрины dbt с приёмником CDC (обращений)",
     "select abs((select coalesce(sum(cases_cnt), 0) from " + DBT + ".dm_open_cases) - (select count(*) from "
     + ODS + ".ods_complaint_cases))", 0, "строк", "warn"),
    # ---- точность: отражают ли данные реальность (сверка с источником и единицы измерения)
    ("точность", "stg", "stg_cfpb_complaints", "drift(API, 60 дней)", "Доля дней, где последняя сверка с API не сошлась",
     "select count(*) filter (where status <> 'success')::numeric / nullif(count(*), 0) from (select distinct on (ds) ds, status "
     "from " + STG + ".etl_load_log where source = 'reconcile_api' and ds >= current_date - 60 order by ds, id desc) t",
     0, "доля", "warn"),
    ("точность", "datamart", "dm_complaints_daily", "out_of_range(fed_funds_rate)", "Ставка ФРС вне 0..20 (записана в долях, а не в %)",
     "select count(*) filter (where fed_funds_rate < 0 or fed_funds_rate > 20 or (fed_funds_rate > 0 and fed_funds_rate < 0.2))"
     "::numeric / nullif(count(*), 0) from " + DM + ".dm_complaints_daily", 0, "доля", "error"),
    ("точность", "datamart", "dm_bank_size_quarter", "out_of_range(assets_k_usd)", "Активы банка меньше 1 млрд USD (не в тысячах?)",
     "select count(*) filter (where assets_k_usd < 1000000)::numeric / nullif(count(*), 0) from " + DM + ".dm_bank_size_quarter",
     0, "доля", "warn"),
    # ---- актуальность: достаточно ли свежие данные
    ("актуальность", "ods", "ods_complaints", "lag(источник)", "Дней от последнего загруженного дня жалоб до сегодня (норма 22)",
     "select current_date - max(ds) from " + ODS + ".ods_complaints", SCHEDULE_LAG_DAYS + 3, "дней", "error"),
    ("актуальность", "datamart", "dm_complaints_daily", "lag(витрина)", "Часов с последней успешной сборки витрины",
     FRESH_LOG_SQL, 30, "часов", "error"),
    ("актуальность", "dbt", "dm_state_month", "lag(dbt)", "Часов с последней успешной сборки витрин dbt",
     FRESH_LOG_SQL, 30, "часов", "warn"),
    ("актуальность", "cdc", "dm_open_cases", "lag(CDC)", "Часов с последней пересборки слоя CDC",
     FRESH_LOG_SQL, 3, "часов", "warn"),
]
LOG_SOURCE = {"lag(витрина)": "dm_complaints_daily", "lag(dbt)": "dbt_build_marts", "lag(CDC)": "dbt_build_cdc"}

# Точность хранилища: копия в ClickHouse против PostgreSQL (drift сумм). Считаются, только если ClickHouse включён.
CH_DRIFT = [
    ("dm_complaints_daily", DM, "complaints_cnt", "Витрина по дням: ClickHouse против PostgreSQL"),
    ("dm_company_month", DM, "complaints_cnt", "Витрина по компаниям: ClickHouse против PostgreSQL"),
    ("dm_bank_size_quarter", DM, "complaints_cnt", "Витрина размера банка: ClickHouse против PostgreSQL"),
    ("dm_open_cases", DBT, "cases_cnt", "Витрина CDC: ClickHouse против PostgreSQL"),
]


def _status(value, threshold, severity):
    if value is not None and float(value) <= float(threshold):
        return "ok"
    return "fail" if severity == "error" else "warn"


def compute(ds):
    """Посчитать все метрики за день ds и записать в dq_metrics (delete + insert). Возвращает список строк."""
    params = {"products": list(load_cfpb.PRODUCTS), "responses": RESPONSES}
    rows = []
    conn = cp.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(DDL)
        for dim, layer, table, metric, title, sql, thr, unit, sev in PG_METRICS:
            p = dict(params, src=LOG_SOURCE.get(metric))
            try:
                cur.execute("savepoint m")
                cur.execute(sql, p)
                value = cur.fetchone()[0]
                cur.execute("release savepoint m")
            except Exception as error:          # noqa: BLE001 - нет таблицы и т.п.: метрика = null, статус по severity
                cur.execute("rollback to savepoint m")
                print(f"{metric} ({table}): не посчитана: {str(error).splitlines()[0]}")
                value = None
            value = None if value is None else round(float(value), 6)
            rows.append((dim, layer, table, metric, title, value, thr, unit, sev, _status(value, thr, sev)))
        if cp.clickhouse_enabled():
            rows += _clickhouse_drift(cur)
        cur.execute("delete from " + TABLE + " where check_date = %(d)s", {"d": ds})
        for r in rows:
            cur.execute("insert into " + TABLE + " (check_date, dimension, layer, table_name, metric, title, value, threshold, "
                        "unit, severity, status, failed) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (ds, *r, 1 if r[-1] == "fail" else 0))
        conn.commit()
    finally:
        conn.close()
    for r in rows:
        print(f"[{r[-1]:4}] {r[0]:15} {r[2]:22} {r[3]:32} = {r[5]} (порог {r[6]} {r[7]})")
    return rows


def _clickhouse_drift(pg_cur):
    import clickhouse_load
    out = []
    db = clickhouse_load.setting("db", "default")
    for name, schema, col, title in CH_DRIFT:
        try:
            pg_cur.execute(f"select coalesce(sum({col}), 0) from {schema}.{name}")
            src = float(pg_cur.fetchone()[0])
            tgt = float(clickhouse_load.ch(f"SELECT sum({col}) FROM {db}.{name} FINAL") or 0)
            value = round(abs(tgt - src) / src, 6) if src else (0.0 if tgt == 0 else 1.0)
        except Exception as error:              # noqa: BLE001
            print(f"drift {name}: не посчитан: {error}")
            value = None
        out.append(("точность", "clickhouse", name, f"drift({col})", title, value, 0, "доля", "error",
                    _status(value, 0, "error")))
    return out


CH_DDL = """create table if not exists {db}.dq_metrics
(
    check_date  Date,
    dimension   LowCardinality(String),
    layer       LowCardinality(String),
    table_name  String,
    metric      String,
    title       String,
    value       Nullable(Float64),
    threshold   Float64,
    unit        LowCardinality(String),
    severity    LowCardinality(String),
    status      LowCardinality(String),
    failed      UInt8
)
engine = ReplacingMergeTree
order by (check_date, table_name, metric)"""


def load_clickhouse():
    """dq_metrics целиком в ClickHouse (таблица маленькая), сверка строк и числа fail."""
    import clickhouse_load
    clickhouse_load.ch(CH_DDL.format(db=clickhouse_load.setting("db", "default")))
    conn = cp.get_conn()
    try:
        with conn.cursor() as cur:
            return clickhouse_load.load_mart_full(cur, DM, "dq_metrics")
    finally:
        conn.close()


def check(ds):
    """Красная задача, если за день есть метрика со статусом fail (severity = error). warn только печатается."""
    conn = cp.get_conn()
    cur = conn.cursor()
    cur.execute("select status, table_name, metric, value, threshold from " + TABLE + " where check_date = %(d)s "
                "and status <> 'ok' order by status, table_name", {"d": ds})
    bad = cur.fetchall()
    conn.close()
    for status, table, metric, value, thr in bad:
        print(f"{status}: {table}.{metric} = {value} (порог {thr})")
    fails = [f"{t}.{m}={v}" for s, t, m, v, _ in bad if s == "fail"]
    if fails:
        raise RuntimeError("качество данных: " + "; ".join(fails))
    return {"warn": len(bad), "fail": 0}


def do_compute(ds):
    started_at = datetime.now()
    try:
        rows = compute(ds)
    except Exception as error:
        cp.write_log("dq_metrics", ds, started_at, "failed", 0, str(error)[:500])
        raise
    cp.write_log("dq_metrics", ds, started_at, "success", len(rows), None)
    return len(rows)


def do_load_clickhouse(ds):
    from airflow.exceptions import AirflowSkipException
    if not cp.clickhouse_enabled():
        raise AirflowSkipException("ClickHouse выключен (tabyldieva_ch_enabled != 1)")
    started_at = datetime.now()
    try:
        n = load_clickhouse()
    except Exception as error:
        cp.write_log("clickhouse_dq_metrics", ds, started_at, "failed", 0, str(error)[:500])
        raise
    cp.write_log("clickhouse_dq_metrics", ds, started_at, "success", n, None)
    return n


if __name__ == "__main__":                       # из терминала: python3 ingestion/dq_metrics.py 2026-10-29
    import sys
    compute(sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat())
