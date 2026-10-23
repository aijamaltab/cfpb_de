#!/usr/bin/env python3
"""Копии витрин в ClickHouse: создать таблицы (--create) и залить витрины целиком (по умолчанию все шесть).

Зачем: дашборды (Superset) читают все витрины из одного места, из ClickHouse. Витрины DAG (dm_company_month,
dm_bank_size_quarter) дальше по дням ведёт DAG final_tabyldieva_datamart. Витрины dbt (dm_state_month, dm_type_daily) DAG не
строит: после каждого `dbt build` их нужно перезалить этим скриптом (`--only dm_state_month,dm_type_daily`).
Повтор безопасен: таблица очищается и заливается заново; в конце сверяются число строк и суммы метрик с PostgreSQL.

Запуск из папки final_project (пароли вводятся вручную, в файлах их нет):
  read -s -p "PG password: " PGPASSWORD; echo; export PGPASSWORD
  read -s -p "CH password: " CH_PASSWORD; echo; export CH_PASSWORD
  export CH_HOST=clickhouse CH_PORT=8123 CH_USER=admin CH_DB=tabyldieva
  python3 tools/clickhouse_backfill_marts.py --create            # один раз: таблицы из sql/35_clickhouse_marts.sql
  python3 tools/clickhouse_backfill_marts.py                     # залить все витрины
  python3 tools/clickhouse_backfill_marts.py --only dm_state_month,dm_type_daily   # только витрины dbt
"""
import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ingestion"))

import cfpb_pipeline as cp      # noqa: E402
import clickhouse_load          # noqa: E402

DBT_SCHEMA = cp.LOGIN + "_dbt"
SCHEMA_OF = {"dm_company_month": cp.DM, "dm_bank_size_quarter": cp.DM, "dm_state_month": DBT_SCHEMA, "dm_type_daily": DBT_SCHEMA,
             "dm_open_cases": DBT_SCHEMA}


def create_tables():
    """Выполнить запросы из sql/35 по одному (HTTP-интерфейс ClickHouse не принимает несколько запросов сразу)."""
    text = (ROOT / "sql" / "35_clickhouse_marts.sql").read_text(encoding="utf-8")
    text = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
    for statement in re.split(r";\s*(?:\n|$)", text):
        statement = statement.strip()
        if statement:
            clickhouse_load.ch(statement)
            print("создана таблица:", statement.split("\n")[0].replace("create table if not exists", "").strip())


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--create", action="store_true", help="создать таблицы в ClickHouse (sql/35) и выйти")
    ap.add_argument("--only", default="", help="список витрин через запятую (по умолчанию все шесть)")
    args = ap.parse_args()
    if args.create:
        create_tables()
        return
    names = [n for n in args.only.split(",") if n] or list(clickhouse_load.MARTS)
    unknown = [n for n in names if n not in clickhouse_load.MARTS]
    if unknown:
        sys.exit(f"неизвестная витрина: {', '.join(unknown)}; доступны: {', '.join(clickhouse_load.MARTS)}")
    conn = cp.get_conn()
    try:
        with conn.cursor() as cur:
            for name in names:
                cur.execute("select to_regclass(%s)", (SCHEMA_OF[name] + "." + name,))
                if cur.fetchone()[0] is None:
                    print(f"пропущена {name}: в PostgreSQL нет {SCHEMA_OF[name]}.{name} (витрина ещё не создана или не собрана)")
                    continue
                n = clickhouse_load.load_mart_full(cur, SCHEMA_OF[name], name)
                print(f"ClickHouse {clickhouse_load.setting('db', 'default')}.{name}: залито {n} строк, сверка с PostgreSQL прошла")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
