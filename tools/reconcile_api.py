#!/usr/bin/env python3
"""Сверка: сколько жалоб за день говорит API и сколько загружено в stg (по _ds).

  python3 tools/reconcile_api.py --table stg_cfpb_complaints --start 2026-09-01 --end 2026-09-14
"""
import argparse, datetime as dt, os, sys
import pathlib
import psycopg2
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / 'ingestion'))
import cfpb_http
import load_cfpb

BASE = os.environ.get("CFPB_BASE", "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/")


def api_total(ds):
    return load_cfpb.api_total_day(dt.date.fromisoformat(ds))   # сумма по банковским продуктам, как у загрузчика


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="stg_cfpb_complaints")
    ap.add_argument("--start", required=True); ap.add_argument("--end", required=True)
    ap.add_argument("--schema")
    a = ap.parse_args()
    schema = a.schema or os.environ["PGUSER"] + "_stg"
    conn = psycopg2.connect(connect_timeout=10); cur = conn.cursor()
    cur.execute(f'select _ds, count(*) from "{schema}"."{a.table}" where _ds between %s and %s group by 1', (a.start, a.end))
    stg = {r[0]: r[1] for r in cur.fetchall()}
    conn.close()
    d, end, bad, ta, ts = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end), 0, 0, 0
    print(f"{'день':<12}{'в API':>9}{'в stg':>9}{'разница':>9}")
    while d <= end:
        f, s = api_total(d.isoformat()), stg.get(d, 0)
        ta += f; ts += s; bad += f != s
        print(f"{d!s:<12}{f:>9}{s:>9}{s - f:>9}{'' if f == s else '  <-- РАСХОЖДЕНИЕ'}")
        d += dt.timedelta(days=1)
    print(f"{'итого':<12}{ta:>9}{ts:>9}{ts - ta:>9}")
    if bad:
        print(f"Расхождений: {bad} дн. Перезагрузите эти дни через run_window.py. Если API задним числом добавил жалобы, это нормально, но объясните."); sys.exit(1)
    print("Сверка сошлась.")


if __name__ == "__main__":
    main()
