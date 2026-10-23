"""Общие функции пайплайна CFPB: stg, сверка, ods, справочники, проверки. Их вызывают оба DAG.

Лежит рядом со скриптами загрузки (final_project/ingestion), DAG-файлы только расставляют задачи.
"""
import os
from datetime import date, datetime

import psycopg2

import common          # write_rows, table_columns
import load_cfpb       # fetch_day, validate: те же функции, что мы запускали руками

LOGIN = "tabyldieva"
CONN_ID = "aiacademy_pg_" + LOGIN                  # Airflow: Admin -> Connections (пароль хранится только там)
PG_HOST = os.environ.get("PGHOST", "postgres")
PG_DB = os.environ.get("PGDATABASE", "aiacademy")
STG = LOGIN + "_stg"
ODS = LOGIN + "_ods"
DM = LOGIN + "_datamart"
STG_TABLE = "stg_cfpb_complaints"
MAX_ROWS_PER_RUN = 100_000       # правило курса: через Airflow не больше 100 тыс. строк за запуск
LATE_DAYS = 14                   # жалобы публикуются с опозданием: объём дней младше этого срока по среднему не проверяем


def get_conn():
    """Подключение к PostgreSQL. Пароля в коде нет.

    В терминале: переменные PGHOST/PGUSER/PGDATABASE/PGPASSWORD (пароль вводится через read -s).
    В Airflow: Connection с id aiacademy_pg_<логин> (Admin -> Connections, тип Postgres).
    """
    if os.environ.get("PGPASSWORD"):
        conn = psycopg2.connect(host=PG_HOST, port=int(os.environ.get("PGPORT", "5432")),
                                user=os.environ.get("PGUSER", LOGIN), dbname=PG_DB, connect_timeout=10)
    else:
        try:
            from airflow.hooks.base import BaseHook
        except ImportError:
            raise RuntimeError("PGPASSWORD не задан, а Airflow недоступен: введите пароль через read -s PGPASSWORD; export PGPASSWORD")
        c = BaseHook.get_connection(CONN_ID)           # нет подключения -> понятная ошибка Airflow
        conn = psycopg2.connect(host=c.host, port=c.port or 5432, user=c.login, password=c.password,
                                dbname=c.schema or PG_DB, connect_timeout=10)
    conn.set_client_encoding("UTF8")                   # русский текст в справочниках не зависит от локали процесса
    return conn


def apply_api_base():
    """Адрес API можно подменить параметром cfpb_base в форме запуска DAG (для проверки НФТ-4: недоступный адрес, например
    https://localhost:9/x). Параметр действует только на этот запуск: в Airflow ничего не остаётся, следующий запуск идёт на
    настоящий адрес. Нет параметра или код вне Airflow -> адрес по умолчанию (или env CFPB_BASE)."""
    base = None
    try:
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        base = (ctx["dag_run"].conf or {}).get("cfpb_base") or (ctx.get("params") or {}).get("cfpb_base")
    except Exception:                                   # noqa: BLE001 - вне задачи Airflow контекста нет
        base = None
    if base:
        load_cfpb.BASE = base


def write_log(source, ds, started_at, status, rows_loaded, error_text):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "insert into " + STG + ".etl_load_log "
        "(source, ds, started_at, finished_at, status, rows_loaded, error) "
        "values (%s, %s, %s, now(), %s, %s, %s)",
        (source, ds, started_at, status, rows_loaded, error_text),
    )
    conn.commit()
    conn.close()
    print(source, ds, status, rows_loaded)


# ---------- stg: API -> stg (delete + insert по _ds) ----------

def do_load_stg(ds):
    apply_api_base()
    day = date.fromisoformat(ds)
    started_at = datetime.now()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cols = common.table_columns(cur, STG, STG_TABLE)
        conn.rollback()
        if not cols:
            raise RuntimeError(f"таблицы {STG}.{STG_TABLE} нет")
        total, recs = load_cfpb.fetch_day(day, 1000, MAX_ROWS_PER_RUN, 30, 3)   # на продукт лимит = 100 тыс.; сумму за запуск проверяем ниже
        if len(recs) > MAX_ROWS_PER_RUN:
            raise RuntimeError(f"за {ds} пришло {len(recs)} строк, больше лимита {MAX_ROWS_PER_RUN} на запуск")
        rows = load_cfpb.validate(day, recs, cols, False)      # пустой день, чужая дата, лишнее поле -> ошибка
        source_file = f"{load_cfpb.BASE}?date_received_min={ds}&date_received_max={ds}"
        n_del, n_ins = common.write_rows(conn, STG, STG_TABLE, cols, rows, day, source_file, "delete_ds")
    except Exception as error:
        conn.rollback()
        conn.close()
        write_log("stg_cfpb_complaints", ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    write_log("stg_cfpb_complaints", ds, started_at, "success", n_ins, None)
    return n_ins


def do_reconcile(ds):
    """Сверка: сколько жалоб за день говорит API и сколько лежит в stg. Результат пишется в журнал."""
    apply_api_base()
    started_at = datetime.now()
    day = date.fromisoformat(ds)
    try:
        api_total = load_cfpb.api_total_day(day)
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("select count(*) from " + STG + "." + STG_TABLE + " where _ds = %(ds)s", {"ds": ds})
        in_stg = cur.fetchone()[0]
        conn.close()
        print(f"{ds}: API {api_total}, stg {in_stg}")
        if api_total != in_stg:
            raise RuntimeError(f"расхождение за {ds}: API {api_total}, stg {in_stg}")
    except Exception as error:
        write_log("reconcile_api", ds, started_at, "failed", 0, str(error)[:500])
        raise
    write_log("reconcile_api", ds, started_at, "success", in_stg, None)


# ---------- ods: stg -> ods (delete + insert по ds) ----------

ODS_COMPLAINTS_DELETE = "delete from " + ODS + ".ods_complaints where ds = %(ds)s"

ODS_COMPLAINTS_INSERT = """
    insert into """ + ODS + """.ods_complaints
        (complaint_id, received_at, sent_to_company_at, product, sub_product, issue, sub_issue, company,
         state, zip_code, company_response, company_public_response, timely, submitted_via, tags, ds)
    select distinct on (complaint_id)
        complaint_id::bigint,
        date_received::timestamptz at time zone 'UTC',
        nullif(date_sent_to_company, '')::timestamptz at time zone 'UTC',
        product,
        nullif(sub_product, ''),
        nullif(issue, ''),
        nullif(sub_issue, ''),
        company,
        nullif(state, ''),
        nullif(zip_code, ''),
        nullif(company_response, ''),
        nullif(company_public_response, ''),
        case timely when 'Yes' then true when 'No' then false end,
        nullif(submitted_via, ''),
        nullif(tags, ''),
        _ds
    from """ + STG + """.stg_cfpb_complaints
    where _ds = %(ds)s
    order by complaint_id, _loaded_at desc
"""


def run_ods_step(source, delete_sql, insert_sql, ds):
    started_at = datetime.now()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(delete_sql, {"ds": ds})
        cur.execute(insert_sql, {"ds": ds})
        rows = cur.rowcount
        conn.commit()
    except Exception as error:
        conn.rollback()
        conn.close()
        write_log(source, ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    write_log(source, ds, started_at, "success", rows, None)
    return rows


def do_load_ods(ds):
    rows = run_ods_step("ods_complaints", ODS_COMPLAINTS_DELETE, ODS_COMPLAINTS_INSERT, ds)
    # проверка: уникальных жалоб в stg за день столько же, сколько строк в ods; ноль = тревога
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("select count(distinct complaint_id) from " + STG + "." + STG_TABLE + " where _ds = %(ds)s", {"ds": ds})
    in_stg = cur.fetchone()[0]
    cur.execute("select count(*) from " + ODS + ".ods_complaints where ds = %(ds)s", {"ds": ds})
    in_ods = cur.fetchone()[0]
    conn.close()
    print(f"{ds}: stg {in_stg}, ods {in_ods}")
    if in_ods == 0 or in_ods != in_stg:
        raise RuntimeError(f"ods не сошёлся за {ds}: stg {in_stg}, ods {in_ods}")
    return rows


# ---------- справочники ods: truncate + insert (пересборка целиком) ----------

def do_load_ref(ds):
    started_at = datetime.now()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("truncate " + ODS + ".ods_products")
        cur.execute(
            "insert into " + ODS + ".ods_products (product, sub_product, first_seen_ds, last_seen_ds, complaints_cnt) "
            "select product, coalesce(sub_product, ''), min(ds), max(ds), count(*) "
            "from " + ODS + ".ods_complaints group by 1, 2"
        )
        n_products = cur.rowcount
        cur.execute("truncate " + ODS + ".ods_companies")
        cur.execute(
            "insert into " + ODS + ".ods_companies (company, first_seen_ds, last_seen_ds, complaints_cnt) "
            "select company, min(ds), max(ds), count(*) from " + ODS + ".ods_complaints group by 1"
        )
        n_companies = cur.rowcount
        conn.commit()
    except Exception as error:
        conn.rollback()
        conn.close()
        write_log("ods_products", ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    write_log("ods_products", ds, started_at, "success", n_products, None)
    write_log("ods_companies", ds, started_at, "success", n_companies, None)
    return {"products": n_products, "companies": n_companies}


# ---------- витрины dm: delete + insert за день и за месяц ----------

DM_DAILY_DELETE = "delete from " + DM + ".dm_complaints_daily where ds = %(ds)s"
DM_DAILY_INSERT = """
    insert into """ + DM + """.dm_complaints_daily
        (ds, product, complaints_cnt, timely_cnt, untimely_cnt, in_progress_cnt, relief_cnt, fed_funds_rate)
    select o.ds, o.product, count(*),
           count(*) filter (where o.timely),
           count(*) filter (where o.timely is false),
           count(*) filter (where o.company_response = 'In progress'),
           count(*) filter (where o.company_response in ('Closed with monetary relief', 'Closed with non-monetary relief')),
           (select r.fed_funds_rate from """ + ODS + """.ods_fred_dff r where r.ds = o.ds)
    from """ + ODS + """.ods_complaints o
    where o.ds = %(ds)s
    group by o.ds, o.product
"""
DM_MONTH_DELETE = "delete from " + DM + ".dm_company_month where month = date_trunc('month', %(ds)s::date)::date"
DM_MONTH_INSERT = """
    insert into """ + DM + """.dm_company_month (month, company, company_type, complaints_cnt, untimely_cnt, relief_cnt)
    select date_trunc('month', %(ds)s::date)::date, o.company, coalesce(c.company_type, 'other'), count(*),
           count(*) filter (where o.timely is false),
           count(*) filter (where o.company_response in ('Closed with monetary relief', 'Closed with non-monetary relief'))
    from """ + ODS + """.ods_complaints o
    left join """ + ODS + """.ods_dim_company c on c.company_cfpb = o.company
    where o.ds >= date_trunc('month', %(ds)s::date)::date
      and o.ds <  (date_trunc('month', %(ds)s::date) + interval '1 month')::date
    group by o.company, coalesce(c.company_type, 'other')
"""


def do_build_dm(ds):
    """Витрины дня и месяца этого дня. Всё в одной транзакции."""
    started_at = datetime.now()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(DM_DAILY_DELETE, {"ds": ds})
        cur.execute(DM_DAILY_INSERT, {"ds": ds})
        n_daily = cur.rowcount
        cur.execute(DM_MONTH_DELETE, {"ds": ds})
        cur.execute(DM_MONTH_INSERT, {"ds": ds})
        n_month = cur.rowcount
        # сверка: сумма по витрине дня = строк в ods за день
        cur.execute("select coalesce(sum(complaints_cnt), 0) from " + DM + ".dm_complaints_daily where ds = %(ds)s", {"ds": ds})
        in_dm = cur.fetchone()[0]
        cur.execute("select count(*) from " + ODS + ".ods_complaints where ds = %(ds)s", {"ds": ds})
        in_ods = cur.fetchone()[0]
        if in_dm != in_ods:
            raise RuntimeError(f"витрина не сошлась за {ds}: dm {in_dm}, ods {in_ods}")
        conn.commit()
    except Exception as error:
        conn.rollback()
        conn.close()
        write_log("dm_complaints_daily", ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    write_log("dm_complaints_daily", ds, started_at, "success", n_daily, None)
    write_log("dm_company_month", ds, started_at, "success", n_month, None)
    return {"daily": n_daily, "month": n_month}


# ---------- проверки качества дня ----------

DQ_CHECKS = {
    # имя проверки -> запрос, который должен вернуть 0
    "пустые company/product": "select count(*) from " + ODS + ".ods_complaints where ds = %(ds)s "
                              "and (coalesce(company, '') = '' or coalesce(product, '') = '')",
    "received_at не в день ds": "select count(*) from " + ODS + ".ods_complaints where ds = %(ds)s "
                                "and received_at::date <> ds",
    "sent_to_company раньше received": "select count(*) from " + ODS + ".ods_complaints where ds = %(ds)s "
                                       "and sent_to_company_at < received_at - interval '1 day'",
    "жалоба в двух днях": "select count(*) from " + ODS + ".ods_complaints a where a.ds = %(ds)s and exists "
                          "(select 1 from " + ODS + ".ods_complaints b where b.complaint_id = a.complaint_id and b.ds <> a.ds)",
}


def do_dq_check(ds):
    started_at = datetime.now()
    conn = get_conn()
    cur = conn.cursor()
    problems = []
    for name, sql in DQ_CHECKS.items():
        cur.execute(sql, {"ds": ds})
        bad = cur.fetchone()[0]
        print(f"{ds}: {name}: {bad}")
        if bad:
            problems.append(f"{name}={bad}")
    if load_cfpb.PRODUCTS_ON:      # продукт вне списка банковских
        cur.execute(
            "select product, count(*) from " + ODS + ".ods_complaints where ds = %(ds)s "
            "and product <> all(%(products)s) group by 1",
            {"ds": ds, "products": list(load_cfpb.PRODUCTS)},
        )
        for product, cnt in cur.fetchall():
            problems.append(f"продукт вне списка: {product}={cnt}")
    # объём дня против среднего за предыдущие дни (если история уже есть): ниже 10% = тревога.
    # Свежие дни не проверяем по объёму: CFPB публикует жалобы с опозданием, и вчерашний день всегда неполон.
    # Это свойство источника, а не ошибка конвейера; остальные проверки (ключи, даты, пустые поля) работают всегда.
    cur.execute("select count(*) from " + ODS + ".ods_complaints where ds = %(ds)s", {"ds": ds})
    today = cur.fetchone()[0]
    cur.execute(
        "select avg(n) from (select count(*) n from " + ODS + ".ods_complaints where ds < %(ds)s group by ds) t",
        {"ds": ds},
    )
    avg_prev = cur.fetchone()[0]
    conn.close()
    print(f"{ds}: строк {today}, среднее по предыдущим дням {avg_prev}")
    age_days = (date.today() - datetime.strptime(str(ds), "%Y-%m-%d").date()).days
    if age_days < LATE_DAYS:
        print(f"{ds}: свежий день ({age_days} дн.), объём не проверяем: жалобы публикуются с опозданием")
    elif avg_prev and today < 0.1 * float(avg_prev):
        problems.append(f"объём дня {today} < 10% среднего {float(avg_prev):.0f}")
    if problems:
        write_log("dq_check", ds, started_at, "failed", len(problems), "; ".join(problems)[:500])
        raise RuntimeError(f"DQ {ds}: " + "; ".join(problems))
    write_log("dq_check", ds, started_at, "success", len(DQ_CHECKS), None)


# ---------- ClickHouse: ods -> витрина-копия для быстрой аналитики ----------

def clickhouse_enabled():
    import clickhouse_load
    return clickhouse_load.setting("enabled", "0") == "1"


def do_load_clickhouse(ds):
    import clickhouse_load
    started_at = datetime.now()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            rows = clickhouse_load.load_day(cur, ODS, ds)
    except Exception as error:
        conn.close()
        write_log("clickhouse_complaints", ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    write_log("clickhouse_complaints", ds, started_at, "success", rows, None)
    return rows
