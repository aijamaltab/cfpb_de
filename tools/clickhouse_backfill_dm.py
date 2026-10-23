#!/usr/bin/env python3
"""Первичная заливка ВСЕЙ истории витрины dm_complaints_daily в ClickHouse (один раз, после sql/32_clickhouse_dm.sql).

Дальше витрину по дням ведёт DAG final_tabyldieva_datamart. Повторный запуск безопасен (таблица очищается и заливается заново),
в конце сверяются число строк и сумма жалоб с PostgreSQL.

Запуск из папки final_project (пароли вводятся вручную, в файлах их нет):
  read -s -p "PG password: " PGPASSWORD; echo; export PGPASSWORD
  read -s -p "CH password: " CH_PASSWORD; echo; export CH_PASSWORD
  CH_HOST=clickhouse CH_PORT=8123 CH_USER=admin CH_DB=tabyldieva python3 tools/clickhouse_backfill_dm.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion"))

import cfpb_pipeline as cp      # noqa: E402
import clickhouse_load          # noqa: E402

conn = cp.get_conn()
try:
    with conn.cursor() as cur:
        n = clickhouse_load.load_mart_all(cur, cp.DM)
finally:
    conn.close()
print(f"ClickHouse {clickhouse_load.setting('db', 'default')}.{clickhouse_load.MART_TABLE}: залито {n} строк, сверка с PostgreSQL прошла")
