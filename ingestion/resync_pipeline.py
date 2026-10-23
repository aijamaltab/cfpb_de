"""Досинхронизация: CFPB публикует жалобы за прошлые дни с опозданием (и иногда снимает опубликованные),
поэтому загруженный день со временем расходится с API. Здесь такие дни находят и перезагружают.

Как ищем: окно последних LOOKBACK_DAYS дней режется на блоки по BLOCK_DAYS дней. Для блока сравниваем сумму API
(по продуктам контура) с числом строк в stg; совпал - пропускаем блок целиком (экономит запросы), разошёлся - проверяем
каждый день блока. Оговорка: если в блоке одна жалоба добавилась, а другая снялась, сумма совпадёт и блок пропустится.
Как лечим: для дня с расхождением заново выполняется вся цепочка контура (stg -> сверка -> ods -> ... -> проверки
-> ClickHouse, если включён). Все шаги идемпотентны (delete + insert по ds), повтор безопасен.
Лимит курса: за запуск не больше MAX_ROWS_PER_RUN строк; если дней больше, остаток доделает следующий запуск.
"""
import time
from datetime import date, datetime, timedelta

import bureau_pipeline
import cfpb_pipeline
import load_cfpb
from cfpb_pipeline import get_conn, write_log, STG, MAX_ROWS_PER_RUN

BLOCK_DAYS = 7
LOOKBACK_DAYS = 240

CONTOURS = {
    "bank": {
        "module": cfpb_pipeline,
        "products": lambda: list(load_cfpb.PRODUCTS),
        "window": (date(2023, 10, 1), date(2026, 9, 30)),
        "steps": ["do_load_stg", "do_reconcile", "do_load_ods", "do_load_ref", "do_build_dm", "do_dq_check"],
    },
    "bureau": {
        "module": bureau_pipeline,
        "products": lambda: list(bureau_pipeline.BUREAU_PRODUCTS),
        "window": (date(2026, 7, 1), date(2026, 9, 30)),
        "steps": ["do_load_stg", "do_reconcile", "do_load_ods", "do_dq_check"],
    },
}


def _api_range_total(d0, d1, products):
    total = 0
    for p in products:
        params = load_cfpb.day_params(d0, 1, None, [p])
        params["date_received_max"] = d1.isoformat()
        total += load_cfpb.get_page(params, 30, 3)[0]
        time.sleep(0.2)
    return total


def _stg_counts(table, d0, d1):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"select _ds, count(*) from {STG}.{table} where _ds between %s and %s group by 1", (d0, d1))
    res = {r[0]: r[1] for r in cur.fetchall()}
    conn.close()
    return res


def find_diff_days(contour, d0, d1):
    """Дни окна, где API и stg расходятся: [(день, в API, в stg)]."""
    cfg = CONTOURS[contour]
    products = cfg["products"]()
    table = cfg["module"].STG_TABLE
    counts = _stg_counts(table, d0, d1)
    out, d = [], d0
    while d <= d1:
        b_end = min(d + timedelta(days=BLOCK_DAYS - 1), d1)
        api_block = _api_range_total(d, b_end, products)
        stg_block = sum(v for k, v in counts.items() if d <= k <= b_end)
        if api_block != stg_block:
            day = d
            while day <= b_end:
                a = load_cfpb.api_total_day(day, products=products)
                s = counts.get(day, 0)
                if a != s:
                    out.append((day, a, s))
                time.sleep(0.2)
                day += timedelta(days=1)
        d = b_end + timedelta(days=1)
    return out


def resync(contour, today=None, lookback=LOOKBACK_DAYS):
    cfg = CONTOURS[contour]
    mod = cfg["module"]
    today = today or date.today()
    w0, w1 = cfg["window"]
    d1 = min(w1, today - timedelta(days=1))
    d0 = max(w0, d1 - timedelta(days=lookback - 1))
    source = f"resync_{contour}"
    started_at = datetime.now()
    ds_log = today.isoformat()
    try:
        diff = find_diff_days(contour, d0, d1)
        print(f"{contour}: проверено {d0}..{d1}, дней с расхождением: {len(diff)}")
        for day, a, s in diff:
            print(f"  {day}: API {a}, stg {s}, разница {s - a:+d}")
        rows_total, fixed, failed, stopped = 0, [], [], False
        for day, a, s in diff:
            if rows_total >= MAX_ROWS_PER_RUN:
                stopped = True
                break
            ds = day.isoformat()
            try:
                for name in cfg["steps"]:
                    res = getattr(mod, name)(ds)
                    if name == "do_load_stg" and isinstance(res, int):
                        rows_total += res
                if cfpb_pipeline.clickhouse_enabled():
                    mod.do_load_clickhouse(ds)
                    if contour == "bank":
                        # витрина dm_complaints_daily в ClickHouse (ФТ-7) должна сходиться с PostgreSQL и после досинхронизации
                        import datamart_pipeline
                        datamart_pipeline.do_load_clickhouse_mart(ds)
                fixed.append(ds)
                print(f"  {ds}: перезагружен")
            except Exception as error:
                failed.append(f"{ds}: {str(error)[:120]}")
                print(f"  {ds}: ОШИБКА {error}")
        msg = f"дней с расхождением {len(diff)}, перезагружено {len(fixed)}"
        if stopped:
            msg += f", остановлено по лимиту {MAX_ROWS_PER_RUN} строк (остаток доделает следующий запуск)"
        if failed:
            msg += "; ошибки: " + " | ".join(failed)
            raise RuntimeError(msg)
    except Exception as error:
        write_log(source, ds_log, started_at, "failed", 0, str(error)[:500])
        raise
    write_log(source, ds_log, started_at, "success", len(fixed), msg[:500] if stopped else None)
    print(msg)
    return len(fixed)
