"""Квартальные показатели банков США из открытого API FDIC (/financials, без ключа).

API по кварталам (конец квартала = REPDTE) -> stg_fdic_financials -> сверка с meta.total по каждому кварталу ->
ods_fdic_financials -> проверки. Один запуск перезагружает все кварталы окна (около 4,5 тыс. банков на квартал,
всего заведомо меньше лимита 100 тыс. на запуск): повторный запуск безопасен, новый квартал подхватывается сам.
Последние кварталы могут быть ещё не опубликованы: пустой квартал допустим только в конце ряда, не посередине.
"""
import calendar
import time
from datetime import date, datetime

import cfpb_http
import common
from cfpb_pipeline import get_conn, write_log, STG, ODS, MAX_ROWS_PER_RUN

BASE = "https://api.fdic.gov/banks/financials"
FIELDS = ["CERT", "REPDTE", "ASSET", "DEP", "NETINC", "ROA", "ROE", "EQ", "NUMEMP"]
CORE = ["CERT", "REPDTE", "ASSET", "DEP"]          # без них запись бессмысленна
STG_TABLE = "stg_fdic_financials"
ODS_TABLE = "ods_fdic_financials"
PAGE = 5000
FIRST = date(2023, 3, 31)
MIN_BANKS_PER_QUARTER = 3000
SRC = "fdicfin_"


def quarter_ends(today=None):
    """Концы кварталов от FIRST до последнего завершённого."""
    today = today or date.today()
    out, y, m = [], FIRST.year, 3
    while True:
        d = date(y, m, calendar.monthrange(y, m)[1])
        if d >= today:
            break
        out.append(d)
        m += 3
        if m > 12:
            y, m = y + 1, 3
    return out


def fetch_quarter(qend):
    """Все банки за квартал. Падает, если получено не столько, сколько обещал meta.total."""
    rows, offset = [], 0
    while True:
        r = cfpb_http.get(BASE, {"filters": f"REPDTE:{qend.strftime('%Y%m%d')}", "fields": ",".join(FIELDS),
                                 "limit": PAGE, "offset": offset, "sort_by": "CERT", "sort_order": "ASC"}, 60, 3)
        if r.status_code != 200:
            raise RuntimeError(f"FDIC financials {qend}: HTTP {r.status_code}: {r.text[:150]}")
        try:
            data = r.json()
            total = int(data["meta"]["total"])
            page = [item["data"] for item in data["data"]]
        except (KeyError, TypeError, ValueError):
            raise RuntimeError(f"{qend}: ответ FDIC не похож на ожидаемый (нет meta.total или data)")
        rows.extend(page)
        offset += PAGE
        if not page or len(rows) >= total:
            break
    if len(rows) != total:
        raise RuntimeError(f"{qend}: получили {len(rows)}, а API обещал {total}")
    return total, rows


def check_records(qend, recs):
    for f in CORE:
        missing = sum(1 for x in recs if x.get(f) in (None, ""))
        if missing:
            raise RuntimeError(f"{qend}: у {missing} записей нет поля {f} (контракт нарушен)")
    bad_date = sum(1 for x in recs if str(x["REPDTE"]) != qend.strftime("%Y%m%d"))
    if bad_date:
        raise RuntimeError(f"{qend}: у {bad_date} записей другая дата отчёта")


def do_load_stg(ds):
    day = date.fromisoformat(ds)
    started_at = datetime.now()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cols = common.table_columns(cur, STG, STG_TABLE)
        conn.rollback()
        if not cols:
            raise RuntimeError(f"таблицы {STG}.{STG_TABLE} нет (запусти sql/22_fdic_financials.sql)")
        all_rows, per_q, gaps = [], {}, []
        for qend in quarter_ends(day):
            total, recs = fetch_quarter(qend)
            if total == 0:
                gaps.append(qend)           # квартал ещё не опубликован или дыра
                continue
            if gaps:
                raise RuntimeError(f"пустой квартал {gaps[0]} посередине ряда (есть более поздний {qend})")
            if total < MIN_BANKS_PER_QUARTER:
                raise RuntimeError(f"{qend}: всего {total} банков (ожидаем не меньше {MIN_BANKS_PER_QUARTER})")
            check_records(qend, recs)
            for x in recs:
                low = {k.lower(): v for k, v in x.items()}
                all_rows.append(["" if low.get(c) is None else str(low[c]) for c in cols])
            per_q[qend] = total
            print(f"{qend}: {total} банков")
            time.sleep(0.3)
        if not per_q:
            raise RuntimeError("ни одного квартала не получено")
        if len(all_rows) > MAX_ROWS_PER_RUN:
            raise RuntimeError(f"{len(all_rows)} строк больше лимита {MAX_ROWS_PER_RUN} на запуск")
        unknown = [c for c in cols if c.upper() not in FIELDS]
        if unknown:
            raise RuntimeError(f"в stg есть колонки, которых нет в запросе: {unknown}")
        n_del, n_ins = common.write_rows(conn, STG, STG_TABLE, cols, all_rows, day, f"{BASE}?filters=REPDTE:*", "truncate")
        if n_ins != sum(per_q.values()):
            raise RuntimeError(f"в stg {n_ins}, API обещал {sum(per_q.values())}")
    except Exception as error:
        conn.rollback()
        conn.close()
        write_log(SRC + "stg", ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    write_log(SRC + "stg", ds, started_at, "success", n_ins, None)
    return n_ins


def _num(c, typ="bigint"):
    return f"nullif({c}, '')::numeric::{typ}"


ODS_INSERT = f"""
    insert into {ODS}.{ODS_TABLE}
        (cert, report_date, assets_k_usd, deposits_k_usd, net_income_k_usd, roa, roe, equity_k_usd, employees, ds)
    select distinct on (cert, repdte)
        cert::bigint, to_date(repdte, 'YYYYMMDD'),
        {_num('asset')}, {_num('dep')}, {_num('netinc')},
        {_num('roa', 'numeric(10,4)')}, {_num('roe', 'numeric(10,4)')}, {_num('eq')}, {_num('numemp', 'int')},
        _ds
    from {STG}.{STG_TABLE}
    where _ds = %(ds)s
    order by cert, repdte, _loaded_at desc
"""


def do_load_ods(ds):
    started_at = datetime.now()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("delete from " + ODS + "." + ODS_TABLE)
        cur.execute(ODS_INSERT, {"ds": ds})
        rows = cur.rowcount
        cur.execute("select count(*) from (select distinct cert, repdte from " + STG + "." + STG_TABLE +
                    " where _ds = %(ds)s) s", {"ds": ds})
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


T = ODS + "." + ODS_TABLE
DQ = {
    "отрицательные активы или депозиты": f"select count(*) from {T} where assets_k_usd < 0 or deposits_k_usd < 0",
    "нет активов": f"select count(*) from {T} where assets_k_usd is null",
    "квартал с малым числом банков": f"select count(*) from (select report_date from {T} group by 1 having count(*) < {MIN_BANKS_PER_QUARTER}) s",
    "пропущенный квартал": f"select count(*) from (select report_date, lag(report_date) over (order by report_date) prev from (select distinct report_date from {T}) d) s where prev is not null and (report_date - prev) not between 88 and 93",
    "дата не конец квартала": f"select count(*) from {T} where extract(month from report_date) not in (3, 6, 9, 12) or report_date <> (date_trunc('month', report_date) + interval '1 month - 1 day')::date",
}


def do_dq_check(ds):
    started_at = datetime.now()
    conn = get_conn()
    cur = conn.cursor()
    problems = []
    for name, sql in DQ.items():
        cur.execute(sql)
        bad = cur.fetchone()[0]
        print(f"{ds}: {name}: {bad}")
        if bad:
            problems.append(f"{name}={bad}")
    conn.close()
    if problems:
        write_log(SRC + "dq", ds, started_at, "failed", len(problems), "; ".join(problems)[:500])
        raise RuntimeError(f"DQ {ds}: " + "; ".join(problems))
    write_log(SRC + "dq", ds, started_at, "success", len(DQ), None)