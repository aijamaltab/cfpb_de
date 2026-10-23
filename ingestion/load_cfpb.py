"""Загрузка ОДНОГО дня жалоб CFPB в stg (режим delete + insert по _ds).

Пример:
  python3 ingestion/load_cfpb.py --table stg_cfpb_complaints --ds 2026-09-01

По образцу load_api.py ментора, но самостоятельный (load_api.py не нужен), под форму ответа CFPB: жалобы лежат в hits.hits[]._source,
число жалоб в hits.total.value, страницы листаются курсором search_after (frm у этого API не работает).
Повторный запуск того же дня даёт тот же результат (идемпотентность). Всё пишется в etl_load_log.

Громкие поломки (лучше упасть, чем соврать):
  - ответ не JSON или нет hits.total.value
  - строк получено не столько, сколько обещал API
  - у жалобы date_received не равна запрошенному дню
  - в ответе есть поле без колонки в таблице, или не хватает ожидаемого поля
  - пустой день (если не указан --allow-empty)
  - за день больше --max-per-query строк (не грузим часть молча)
  - один и тот же complaint_id пришёл дважды (курсор сломан)
ВАЖНО: имена параметров date_received_min/max, frm, size, sort в API проверяются скриптом tools/probe_api.py.
"""
import argparse
import datetime as dt
import json
import os
import sys
import time

import common
import cfpb_http

PRODUCTS_ON = True   # --all-products выключает фильтр и проверку

BASE = os.environ.get("CFPB_BASE", "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/")


# Банковские продукты (решение по проекту: кредитные бюро, коллекторы и debt management не грузим).
# Названия ровно как в данных API (проверено выборкой за 2026-09-01/02 в ods_complaints).
PRODUCTS = [
    "Credit card",
    "Checking or savings account",
    "Mortgage",
    "Money transfer, virtual currency, or money service",
    "Vehicle loan or lease",
    "Payday loan, title loan, personal loan, or advance loan",
    "Student loan",
    "Prepaid card",
]


def flatten_obj(obj, prefix=""):
    """Жалоба -> плоский словарь {колонка: текст}. {"a":{"b":1}} -> {"a_b": "1"}.

    Имена полей приводятся к нижнему регистру. Списки внутри объекта сохраняются как текст JSON,
    None остаётся None. У CFPB жалоба в hits.hits[]._source уже плоская, вложенность оставлена на случай изменения API.
    Функция из шаблона курса (load_api.py), перенесена сюда, чтобы загрузчик не зависел от чужого файла.
    """
    out = {}
    for k, v in obj.items():
        name = f"{prefix}{k}".lower()
        if isinstance(v, dict):
            out.update(flatten_obj(v, name + "_"))
        elif isinstance(v, list):
            out[name] = json.dumps(v, ensure_ascii=False)
        elif v is None:
            out[name] = None
        else:
            out[name] = str(v)
    return out


def day_params(ds, size, after=None, products=None):
    products = (PRODUCTS if PRODUCTS_ON else []) if products is None else products
    # frm НЕ используем: проверка показала, что смещение не меняет результат. Листаем курсором search_after.
    p = {"size": size, "no_aggs": "true", "sort": "created_date_desc",
         "date_received_min": ds.isoformat(), "date_received_max": ds.isoformat()}
    if products:
        p["product"] = list(products)   # фильтр на стороне API; ниже validate всё равно проверяет product в ответе
    if after:
        p["search_after"] = after
    return p


def get_page(params, timeout, retries):
    r = cfpb_http.get(BASE, params, timeout, retries)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:150]}")
    try:
        data = r.json()
    except ValueError:
        raise RuntimeError("ответ не JSON (content-type: %s)" % r.headers.get("content-type"))
    try:
        total = int(data["hits"]["total"]["value"])
        hits = data["hits"]["hits"]
    except (KeyError, TypeError, ValueError):
        raise RuntimeError("в ответе нет hits.total.value или hits.hits: формат API изменился?")
    return total, hits


def _fetch_one(ds, products, page_size, max_per_query, timeout, retries, pause, seen):
    """Один запрос-фильтр (один продукт или без фильтра) страницами по курсору search_after."""
    recs, after, total = [], None, None
    while True:
        total, hits = get_page(day_params(ds, page_size, after, products), timeout, retries)
        if total > max_per_query:
            raise RuntimeError(f"API говорит {total} строк за {ds} ({products or 'все продукты'}), больше --max-per-query={max_per_query}. "
                               "Часть данных молча не грузим: увеличьте лимит осознанно или режьте запрос.")
        if not hits:
            break
        for h in hits:
            cid = h["_source"].get("complaint_id")
            if cid in seen:
                raise RuntimeError(f"complaint_id {cid} пришёл дважды: курсор листает неправильно")
            seen.add(cid)
            recs.append(h["_source"])
        last = hits[-1].get("sort")
        if not last:
            raise RuntimeError("у записи нет поля sort: курсор недоступен")
        after = "_".join(str(x) for x in last)
        if len(hits) < page_size or len(recs) >= total:
            break
        time.sleep(pause)
    if len(recs) != total:
        raise RuntimeError(f"получили {len(recs)} уникальных строк ({products or 'все продукты'}), а API обещал {total}")
    return total, recs


def fetch_day(ds, page_size, max_per_query, timeout, retries, pause=0.3, products=None):
    """Весь день. С фильтром по продуктам API не принимает список из 8 значений сразу (HTTP 400: product > 200 символов),
    поэтому ходим по одному продукту за запрос и складываем, а общее число сверяем с суммой обещаний API."""
    seen, recs, total = set(), [], 0
    plist = PRODUCTS if products is None else products      # products=... : другой контур (кредитные бюро)
    groups = [[p] for p in plist] if PRODUCTS_ON else [[]]
    for g in groups:
        t, r = _fetch_one(ds, g, page_size, max_per_query, timeout, retries, pause, seen)
        total += t
        recs.extend(r)
        time.sleep(pause)
    return total, recs


def api_total_day(ds, timeout=30, retries=3, products=None):
    """Сколько жалоб за день обещает API (сумма по продуктам-фильтрам). Для сверки с stg."""
    plist = PRODUCTS if products is None else products
    groups = [[p] for p in plist] if PRODUCTS_ON else [[]]
    return sum(get_page(day_params(ds, 1, None, g), timeout, retries)[0] for g in groups)


def validate(ds, recs, cols, allow_empty, products=None):
    """Проверки перед записью. Возвращает строки для записи (список списков) или бросает RuntimeError."""
    if not recs:
        if allow_empty:
            print(f"ВНИМАНИЕ: за {ds} API вернул 0 строк, разрешено флагом --allow-empty. Старые строки дня будут удалены.")
            return []
        raise RuntimeError(f"пустой день {ds}: это тревога, а не успех (если так и надо, добавьте --allow-empty)")
    flat = [flatten_obj(r) for r in recs]
    keys = {k for rec in flat for k in rec}
    extra, missing = sorted(keys - set(cols)), sorted(set(cols) - keys)
    if extra:
        raise RuntimeError(f"в ответе есть поля без колонок в таблице: {extra}. Добавьте колонки или осознанно проигнорируйте")
    must = {"complaint_id", "date_received", "product", "company"}
    if set(missing) & must:
        raise RuntimeError(f"в ответе пропали ключевые поля: {sorted(set(missing) & must)} (контракт нарушен)")
    if missing:
        print(f"ВНИМАНИЕ: ни в одной строке нет полей {missing} (в API пустые поля могут не приходить). Колонки останутся NULL.")
    if PRODUCTS_ON:
        plist = PRODUCTS if products is None else products
        wrong = [rec.get("product") for rec in flat if rec.get("product") not in plist]
        if wrong:
            raise RuntimeError(f"у {len(wrong)} жалоб product вне списка банковских (примеры: {sorted(set(wrong))[:3]}). Фильтр API не сработал?")
    bad = [rec.get("complaint_id") for rec in flat if (rec.get("date_received") or "")[:10] != ds.isoformat()]
    if bad:
        raise RuntimeError(f"у {len(bad)} жалоб date_received не равна {ds} (примеры: {bad[:3]}). Просили один день, пришёл другой")
    return [[rec.get(c) if rec.get(c) is not None else "" for c in cols] for rec in flat]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--table", default="stg_cfpb_complaints")
    ap.add_argument("--ds", required=True, help="день загрузки ГГГГ-ММ-ДД")
    ap.add_argument("--page-size", type=int, default=1000)
    ap.add_argument("--max-per-query", type=int, default=60000, help="предохранитель: день больше этого числа строк = ошибка")
    ap.add_argument("--timeout", type=float, default=30)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--allow-empty", action="store_true", help="пустой день не считать ошибкой")
    ap.add_argument("--schema", help="схема stg (по умолчанию <PGUSER>_stg)")
    ap.add_argument("--source", default="cfpb_complaints", help="имя источника в журнале")
    ap.add_argument("--all-products", action="store_true", help="не фильтровать по банковским продуктам (грузить всё)")
    ap.add_argument("--dry-run", action="store_true", help="скачать и проверить, но в базу не писать")
    a = ap.parse_args()
    global PRODUCTS_ON
    PRODUCTS_ON = not a.all_products
    try:
        ds = dt.date.fromisoformat(a.ds)
    except ValueError:
        sys.exit(f"ОШИБКА: --ds должен быть ГГГГ-ММ-ДД, а не {a.ds!r}")

    if a.dry_run:
        total, recs = fetch_day(ds, a.page_size, a.max_per_query, a.timeout, a.retries)
        print(f"DRY-RUN {ds}: API обещал {total}, получено уникальных {len(recs)}. Проверки полей и дат:")
        cols = ["complaint_id", "date_received", "date_sent_to_company", "product", "sub_product", "issue", "sub_issue",
                "company", "state", "zip_code", "company_response", "company_public_response", "timely", "submitted_via", "tags"]
        validate(ds, recs, cols, a.allow_empty)
        print("OK, в базу ничего не записано")
        return
    schema = common.schema_name(a.schema)
    conn = common.connect()
    with conn.cursor() as cur:
        cols = common.need_table(cur, schema, a.table)
    conn.rollback()

    source_file = f"{BASE}?date_received_min={ds}&date_received_max={ds}"
    log_id = common.log_start(conn, schema, a.source, ds)
    try:
        print(f"GET {source_file}")
        total, recs = fetch_day(ds, a.page_size, a.max_per_query, a.timeout, a.retries)
        rows = validate(ds, recs, cols, a.allow_empty)
        n_del, n_ins = common.write_rows(conn, schema, a.table, cols, rows, ds, source_file, "delete_ds")
    except Exception as e:
        conn.rollback()
        conn.close()
        common.log_finish(schema, log_id, "failed", error=str(e)[:500])
        sys.exit(f"ОШИБКА: {e}")
    conn.close()
    common.log_finish(schema, log_id, "success", rows=n_ins)
    print(f"{schema}.{a.table}, _ds={ds}: удалено {n_del}, вставлено {n_ins} (API: {total}). Журнал: etl_load_log, id={log_id}")


if __name__ == "__main__":
    main()
