#!/usr/bin/env python3
"""Разовая загрузка ВСЕЙ истории ставки ФРС из data/fred/DFF.csv в stg_fred_dff и ods_fred_dff.

Зачем: DAG за один запуск берёт из файла одну строку (день ds). Для дней, прогнанных раньше (история жалоб 2023-2026),
ставки в базе нет. Этот скрипт кладёт в stg все строки файла (_ds = дата строки), в ods все даты; затем
sql/33_backfill_columns.sql проставляет ставку в витрину. Повторный запуск безопасен (таблицы пересобираются целиком).

Запуск из папки final_project (пароль вводится вручную):
  read -s -p "PG password: " PGPASSWORD; echo; export PGPASSWORD
  PGHOST=postgres PGUSER=tabyldieva PGDATABASE=aiacademy python3 tools/backfill_fred.py
"""
import csv
import io
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion"))

import csv_sources as cs                              # noqa: E402
from cfpb_pipeline import get_conn, write_log, STG, ODS   # noqa: E402

started_at = datetime.now()
rows = cs.read_csv(cs.FRED_FILE, cs.FRED_HEADER)
dates = [r[0] for r in rows]
if not rows:
    sys.exit(f"{cs.FRED_FILE.name}: файл пустой")
if len(set(dates)) != len(dates):
    sys.exit(f"{cs.FRED_FILE.name}: в файле повторяются даты")
last = max(dates)

conn = get_conn()
try:
    buf = io.StringIO()
    w = csv.writer(buf)
    for obs, dff in rows:
        date.fromisoformat(obs)                      # дата в ISO, иначе ошибка до записи
        w.writerow([obs, dff, obs, cs.FRED_FILE.name])
    buf.seek(0)
    with conn.cursor() as cur:
        cur.execute("delete from " + STG + ".stg_fred_dff")
        cur.copy_expert("copy " + STG + ".stg_fred_dff (observation_date, dff, _ds, _source_file) from stdin with (format csv)", buf)
        cur.execute("delete from " + ODS + ".ods_fred_dff")
        cur.execute(
            "insert into " + ODS + ".ods_fred_dff (ds, fed_funds_rate) "
            "select observation_date::date, nullif(nullif(trim(dff), ''), '.')::numeric from " + STG + ".stg_fred_dff")
        n_ods = cur.rowcount
    conn.commit()
except Exception as error:
    conn.rollback()
    conn.close()
    write_log("fred_backfill", last, started_at, "failed", 0, str(error)[:500])
    raise
conn.close()
write_log("fred_backfill", last, started_at, "success", n_ods, None)
print(f"готово: stg {len(rows)} строк, ods {n_ods} строк, даты {min(dates)} .. {last}")
