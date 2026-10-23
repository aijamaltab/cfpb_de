"""Витрины финального проекта: считаются из ods в отдельном DAG final_tabyldieva_datamart.

Витрины (схема <логин>_datamart):
  dm_complaints_daily      зерно: день x продукт          (cfpb_pipeline.do_build_dm)
  dm_company_month         зерно: месяц x компания        (cfpb_pipeline.do_build_dm)
  dm_bank_size_quarter     зерно: квартал x компания      (здесь: пересборка целиком, использует FDIC API)
Все шаги идемпотентны: delete + insert по дню / месяцу или пересборка целиком. Каждый шаг пишет строку в etl_load_log.
"""
from datetime import datetime

from cfpb_pipeline import get_conn, write_log, ODS, DM, clickhouse_enabled  # noqa: F401  (clickhouse_enabled для DAG)

BANK_SIZE_INSERT = """
    insert into """ + DM + """.dm_bank_size_quarter
    with win as (select min(ds) d0, max(ds) d1 from """ + ODS + """.ods_complaints),
    fin as (
        select date_trunc('quarter', f.report_date)::date qs, m.company_cfpb company,
               count(*) banks_cnt, sum(f.assets_k_usd) assets, sum(f.deposits_k_usd) deposits
        from """ + ODS + """.ods_fdic_financials f
        join """ + ODS + """.ods_company_bank m on m.cert = f.cert
        where f.assets_k_usd is not null
        group by 1, 2
    ),
    cmp as (
        select date_trunc('quarter', received_at)::date qs, company, count(*) cnt
        from """ + ODS + """.ods_complaints
        where company in (select company_cfpb from """ + ODS + """.ods_company_bank)
        group by 1, 2
    )
    select fin.qs, fin.company, fin.banks_cnt::int, coalesce(cmp.cnt, 0)::int, fin.assets, fin.deposits,
           round(coalesce(cmp.cnt, 0) / (fin.assets / 1e6), 4),
           round(coalesce(cmp.cnt, 0) / nullif(fin.deposits / 1e6, 0), 4)
    from fin
    left join cmp on cmp.qs = fin.qs and cmp.company = fin.company
    cross join win
    where fin.qs >= win.d0
      and (fin.qs + interval '3 months')::date - 1 <= win.d1
"""


def do_build_bank_size(ds):
    """Витрина «жалобы на банк по кварталам рядом с его размером». Пересборка целиком (delete + insert)."""
    started_at = datetime.now()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("select count(*) from " + ODS + ".ods_fdic_financials")
        if cur.fetchone()[0] == 0:
            raise RuntimeError("ods_fdic_financials пуст: сначала запустите DAG fdic_financials_pipeline (API FDIC)")
        cur.execute("select count(*) from " + ODS + ".ods_company_bank")
        if cur.fetchone()[0] == 0:
            raise RuntimeError("ods_company_bank пуст: справочник грузит основной DAG (задача load_ods_refs)")
        cur.execute("delete from " + DM + ".dm_bank_size_quarter")
        cur.execute(BANK_SIZE_INSERT)
        rows = cur.rowcount
        conn.commit()
    except Exception as error:
        conn.rollback()
        conn.close()
        write_log("dm_bank_size_quarter", ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    write_log("dm_bank_size_quarter", ds, started_at, "success", rows, None)
    return rows


# ---------- проверки витрин (запрос должен вернуть 0) ----------

MONTH = "date_trunc('month', %(ds)s::date)::date"
MART_CHECKS = {
    "daily: пустая витрина дня": "select case when count(*) = 0 then 1 else 0 end from " + DM + ".dm_complaints_daily where ds = %(ds)s",
    "daily: отрицательные значения или в срок + просрочено > всего":
        "select count(*) from " + DM + ".dm_complaints_daily where ds = %(ds)s "
        "and (complaints_cnt < 0 or timely_cnt + untimely_cnt > complaints_cnt)",
    "daily: нет ставки ФРС за день":
        "select count(*) from " + DM + ".dm_complaints_daily where ds = %(ds)s and fed_funds_rate is null",
    "month: сумма месяца не равна ods":
        "select abs(coalesce((select sum(complaints_cnt) from " + DM + ".dm_company_month where month = " + MONTH + "), 0) "
        "- (select count(*) from " + ODS + ".ods_complaints where ds >= " + MONTH + " "
        "and ds < (" + MONTH + " + interval '1 month')::date))",
    "daily и month: суммы за месяц не равны":
        "select abs(coalesce((select sum(complaints_cnt) from " + DM + ".dm_complaints_daily "
        "where ds >= " + MONTH + " and ds < (" + MONTH + " + interval '1 month')::date), 0) "
        "- coalesce((select sum(complaints_cnt) from " + DM + ".dm_company_month where month = " + MONTH + "), 0))",
    "month: нет типа компании":
        "select count(*) from " + DM + ".dm_company_month where month = " + MONTH + " and company_type is null",
    "bank_size: активы <= 0 или отрицательная метрика":
        "select count(*) from " + DM + ".dm_bank_size_quarter where assets_k_usd <= 0 or complaints_per_bn_assets < 0",
}


def do_dq_marts(ds):
    started_at = datetime.now()
    conn = get_conn()
    cur = conn.cursor()
    problems = []
    try:
        for name, sql in MART_CHECKS.items():
            cur.execute(sql, {"ds": ds})
            bad = cur.fetchone()[0]
            print(f"{ds}: {name}: {bad}")
            if bad:
                problems.append(f"{name}={bad}")
    finally:
        conn.close()
    if problems:
        write_log("dq_marts", ds, started_at, "failed", len(problems), "; ".join(problems)[:500])
        raise RuntimeError(f"DQ витрин {ds}: " + "; ".join(problems))
    write_log("dq_marts", ds, started_at, "success", len(MART_CHECKS), None)


# ---------- ClickHouse: витрина dm_complaints_daily за тот же день ----------

def do_load_clickhouse_mart(ds):
    import clickhouse_load
    started_at = datetime.now()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            rows = clickhouse_load.load_mart_day(cur, DM, ds)
    except Exception as error:
        conn.close()
        write_log("clickhouse_dm_complaints_daily", ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    write_log("clickhouse_dm_complaints_daily", ds, started_at, "success", rows, None)
    return rows
