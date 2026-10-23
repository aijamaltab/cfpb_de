#!/usr/bin/env python3
"""Сверка контура кредитных бюро и коллекторов: сколько жалоб за день говорит API и сколько в stg_cfpb_bureau.
Ничего не меняет. По умолчанию весь загруженный период 2026-07-01 .. 2026-09-30.

  nohup python3 -u tools/reconcile_bureau.py > reconcile_bureau.log 2>&1 &
Нужен PGPASSWORD (read -s PGPASSWORD; export PGPASSWORD) ДО запуска, в том же терминале.
В конце печатает список дней с расхождением и готовую строку для Airflow.
"""
import argparse, datetime as dt, os, pathlib, sys
import psycopg2
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "ingestion"))
import load_cfpb

BUREAU_PRODUCTS = ["Credit reporting or other personal consumer reports", "Debt collection"]

ap = argparse.ArgumentParser()
ap.add_argument("--start", default="2026-07-01")
ap.add_argument("--end", default="2026-09-30")
ap.add_argument("--table", default="stg_cfpb_bureau")
a = ap.parse_args()
schema = os.environ["PGUSER"] + "_stg"
conn = psycopg2.connect(connect_timeout=10)
cur = conn.cursor()
cur.execute(f'select _ds, count(*) from "{schema}"."{a.table}" where _ds between %s and %s group by 1', (a.start, a.end))
stg = {r[0]: r[1] for r in cur.fetchall()}
conn.close()
d, end = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)
ta = ts = 0
bad = []
print(f"{'день':<12}{'в API':>9}{'в stg':>9}{'разница':>9}", flush=True)
while d <= end:
    f = load_cfpb.api_total_day(d, products=BUREAU_PRODUCTS)
    s = stg.get(d, 0)
    ta += f; ts += s
    if f != s:
        bad.append((d, s - f))
    print(f"{d!s:<12}{f:>9}{s:>9}{s - f:>9}{'' if f == s else '  <-- РАСХОЖДЕНИЕ'}", flush=True)
    d += dt.timedelta(days=1)
print(f"{'итого':<12}{ta:>9}{ts:>9}{ts - ta:>9}")
if bad:
    print(f"\nРасхождений: {len(bad)} дн.")
    print("Дни:", " ".join(str(x[0]) for x in bad))
    print("Перезагрузить: Airflow, DAG cfpb_bureau_pipeline, очистить (Clear) запуски этих дней.")
else:
    print("\nСверка сошлась.")
print("КОНЕЦ", flush=True)
