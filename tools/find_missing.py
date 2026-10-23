#!/usr/bin/env python3
"""Находит жалобы, которые есть в API за день, но не загружены в stg (и наоборот). Ничего не меняет.

  python3 -u tools/find_missing.py --ds 2026-07-15 2026-07-05 2026-06-20 2026-05-27 2026-03-28
Нужен PGPASSWORD (read -s PGPASSWORD; export PGPASSWORD). Для каждого дня берёт ту же выгрузку API, что и DAG.
"""
import argparse, datetime as dt, os, pathlib, sys
import psycopg2
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "ingestion"))
import load_cfpb

ap = argparse.ArgumentParser()
ap.add_argument("--ds", nargs="+", required=True)
ap.add_argument("--table", default="stg_cfpb_complaints")
a = ap.parse_args()
user = os.environ["PGUSER"]
conn = psycopg2.connect(connect_timeout=10)
cur = conn.cursor()
for ds in a.ds:
    day = dt.date.fromisoformat(ds)
    total, recs = load_cfpb.fetch_day(day, 1000, 100_000, 30, 3)
    api = {str(r["complaint_id"]): r for r in recs}
    cur.execute(f'select complaint_id from "{user}_stg"."{a.table}" where _ds = %s', (day,))
    stg = {str(r[0]) for r in cur.fetchall()}
    miss, extra = sorted(set(api) - stg), sorted(stg - set(api))
    print(f"\n=== {ds}: API {len(api)} (обещал {total}), stg {len(stg)}; нет в stg: {len(miss)}, лишних в stg: {len(extra)}")
    for cid in miss:
        r = api[cid]
        cur.execute(f'select ds, received_at from "{user}_ods".ods_complaints where complaint_id = %s', (int(cid),))
        in_ods = cur.fetchall()
        print(f"  нет в stg: id={cid} product={r.get('product')!r} company={r.get('company')!r} "
              f"date_received={r.get('date_received')} sent={r.get('date_sent_to_company')} state={r.get('state')} "
              f"в ods под другим днём: {in_ods or 'нет'}")
    for cid in extra:
        print(f"  лишний в stg: id={cid}")
conn.close()
