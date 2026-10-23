"""Контур кредитных бюро и коллекторов: API -> stg_cfpb_bureau -> reconcile -> ods_bureau -> проверки.

Те же приёмы, что в основном контуре (delete + insert по дню, громкие ошибки, журнал, сверка с API),
но свои таблицы и свой список продуктов. Основной контур не трогаем.
Объём: в среднем около 21 тыс. строк в день, лимит курса 100 тыс. на запуск соблюдается.
"""
from datetime import date, datetime

import common
import load_cfpb
from cfpb_pipeline import (get_conn, write_log, STG, ODS, MAX_ROWS_PER_RUN)

# Названия продуктов ровно как в API. Проверить: python3 tools/probe_bureau.py --ds 2026-08-17
BUREAU_PRODUCTS = [
    "Credit reporting or other personal consumer reports",
    "Debt collection",
]
STG_TABLE = "stg_cfpb_bureau"
ODS_TABLE = "ods_bureau"
SRC = "bureau_"


def do_load_stg(ds):
    day = date.fromisoformat(ds)
    started_at = datetime.now()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cols = common.table_columns(cur, STG, STG_TABLE)
        conn.rollback()
        if not cols:
            raise RuntimeError(f"таблицы {STG}.{STG_TABLE} нет (запусти sql/12_bureau.sql)")
        total, recs = load_cfpb.fetch_day(day, 1000, MAX_ROWS_PER_RUN, 30, 3, products=BUREAU_PRODUCTS)
        if len(recs) > MAX_ROWS_PER_RUN:
            raise RuntimeError(f"за {ds} пришло {len(recs)} строк, больше лимита {MAX_ROWS_PER_RUN} на запуск")
        rows = load_cfpb.validate(day, recs, cols, False, products=BUREAU_PRODUCTS)
        source_file = f"{load_cfpb.BASE}?date_received_min={ds}&date_received_max={ds}&segment=bureau"
        n_del, n_ins = common.write_rows(conn, STG, STG_TABLE, cols, rows, day, source_file, "delete_ds")
    except Exception as error:
        conn.rollback()
        conn.close()
        write_log(SRC + "stg", ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    write_log(SRC + "stg", ds, started_at, "success", n_ins, None)
    return n_ins


def do_reconcile(ds):
    day = date.fromisoformat(ds)
    api_total = load_cfpb.api_total_day(day, products=BUREAU_PRODUCTS)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("select count(*) from " + STG + "." + STG_TABLE + " where _ds = %(ds)s", {"ds": ds})
    in_stg = cur.fetchone()[0]
    conn.close()
    print(f"{ds}: API {api_total}, stg {in_stg}")
    if api_total != in_stg:
        raise RuntimeError(f"расхождение за {ds}: API {api_total}, stg {in_stg}")


ODS_DELETE = "delete from " + ODS + "." + ODS_TABLE + " where ds = %(ds)s"
ODS_INSERT = """
    insert into """ + ODS + "." + ODS_TABLE + """
        (complaint_id, received_at, sent_to_company_at, product, sub_product, issue, sub_issue, company,
         state, zip_code, company_response, company_public_response, timely, submitted_via, tags, ds)
    select distinct on (complaint_id)
        complaint_id::bigint,
        date_received::timestamptz at time zone 'UTC',
        nullif(date_sent_to_company, '')::timestamptz at time zone 'UTC',
        product, nullif(sub_product, ''), nullif(issue, ''), nullif(sub_issue, ''), company,
        nullif(state, ''), nullif(zip_code, ''), nullif(company_response, ''), nullif(company_public_response, ''),
        case timely when 'Yes' then true when 'No' then false end,
        nullif(submitted_via, ''), nullif(tags, ''), _ds
    from """ + STG + "." + STG_TABLE + """
    where _ds = %(ds)s
    order by complaint_id, _loaded_at desc
"""


def do_load_ods(ds):
    started_at = datetime.now()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(ODS_DELETE, {"ds": ds})
        cur.execute(ODS_INSERT, {"ds": ds})
        rows = cur.rowcount
        cur.execute("select count(distinct complaint_id) from " + STG + "." + STG_TABLE + " where _ds = %(ds)s", {"ds": ds})
        in_stg = cur.fetchone()[0]
        if rows == 0 or rows != in_stg:
            raise RuntimeError(f"ods не сошёлся за {ds}: stg {in_stg}, ods {rows}")
        conn.commit()
    except Exception as error:
        conn.rollback()
        conn.close()
        write_log(SRC + "ods", ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    write_log(SRC + "ods", ds, started_at, "success", rows, None)
    return rows


DQ = {
    "пустые company/product": "select count(*) from " + ODS + "." + ODS_TABLE + " where ds = %(ds)s "
                              "and (coalesce(company, '') = '' or coalesce(product, '') = '')",
    "received_at не в день ds": "select count(*) from " + ODS + "." + ODS_TABLE + " where ds = %(ds)s and received_at::date <> ds",
    "жалоба уже есть в основном контуре": "select count(*) from " + ODS + "." + ODS_TABLE + " b where b.ds = %(ds)s and exists "
                                          "(select 1 from " + ODS + ".ods_complaints c where c.complaint_id = b.complaint_id)",
}


def do_dq_check(ds):
    started_at = datetime.now()
    conn = get_conn()
    cur = conn.cursor()
    problems = []
    for name, sql in DQ.items():
        cur.execute(sql, {"ds": ds})
        bad = cur.fetchone()[0]
        print(f"{ds}: {name}: {bad}")
        if bad:
            problems.append(f"{name}={bad}")
    cur.execute("select product, count(*) from " + ODS + "." + ODS_TABLE + " where ds = %(ds)s and product <> all(%(p)s) group by 1",
                {"ds": ds, "p": list(BUREAU_PRODUCTS)})
    for product, cnt in cur.fetchall():
        problems.append(f"продукт вне списка бюро: {product}={cnt}")
    conn.close()
    if problems:
        write_log(SRC + "dq", ds, started_at, "failed", len(problems), "; ".join(problems)[:500])
        raise RuntimeError(f"DQ {ds}: " + "; ".join(problems))
    write_log(SRC + "dq", ds, started_at, "success", len(DQ), None)


# ---------- ClickHouse: ods_bureau -> tabyldieva.cfpb_bureau (та же логика, что в основном контуре) ----------

def do_load_clickhouse(ds):
    import clickhouse_load
    started_at = datetime.now()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            rows = clickhouse_load.load_day(cur, ODS, ds, ods_table=ODS_TABLE, ch_table="cfpb_bureau")
    except Exception as error:
        conn.close()
        write_log(SRC + "clickhouse", ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    write_log(SRC + "clickhouse", ds, started_at, "success", rows, None)
    return rows
