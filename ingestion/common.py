"""Общие функции ingestion: подключение, журнал etl_load_log, проверка таблицы.

Подключение берётся из переменных окружения стенда (PGHOST, PGUSER, PGDATABASE...):
psycopg2 читает их сам, пароль в коде не нужен.
Схема stg по умолчанию: <PGUSER>_stg (например testqa_stg).
"""
import os
import sys
from datetime import date, timedelta

import psycopg2

SERVICE = ("_ds", "_source_file", "_loaded_at")

# CFPB публикует жалобу после ответа компании или через 15 дней, поэтому свежие дни пусты или неполны (за «вчера» API часто
# отдаёт 0). Запуск по расписанию грузит день на SCHEDULE_LAG_DAYS раньше своей Logical date; 22 выбрано так, чтобы первый
# ночной запуск после 30.09 (окно проекта) продолжил окно с 1 октября без дыр. Ручной запуск грузит ровно выбранную дату.
SCHEDULE_LAG_DAYS = 22


def effective_ds(ds, run_type, lag_days=SCHEDULE_LAG_DAYS):
    """День, который грузит запуск: ds для ручного запуска (в том числе выбранная преподавателем Logical date),
    ds - lag_days для запуска по расписанию (run_type = 'scheduled')."""
    kind = getattr(run_type, "value", run_type)
    if kind == "scheduled":
        return (date.fromisoformat(ds) - timedelta(days=lag_days)).isoformat()
    return ds


def schema_name(arg=None):
    """Схема stg: из --schema или <PGUSER>_stg."""
    if arg:
        return arg
    user = os.environ.get("PGUSER")
    if not user:
        sys.exit("ОШИБКА: не задан PGUSER. Передайте --schema <логин>_stg.")
    return f"{user}_stg"


def connect():
    return psycopg2.connect(connect_timeout=10)


def table_columns(cur, schema, table):
    """Колонки таблицы БЕЗ служебных, в порядке таблицы. Пусто = таблицы нет."""
    cur.execute(
        "select column_name from information_schema.columns "
        "where table_schema = %s and table_name = %s order by ordinal_position",
        (schema, table),
    )
    cols = [r[0] for r in cur.fetchall()]
    return [c for c in cols if c not in SERVICE]


def need_table(cur, schema, table):
    cols = table_columns(cur, schema, table)
    if not cols:
        sys.exit(f"ОШИБКА: таблицы {schema}.{table} нет. Создайте её (sql/02_*.sql, sql/04_*.sql).")
    return cols


def log_start(conn, schema, source, ds):
    """Строка журнала со статусом running. Фиксируем СРАЗУ (commit), чтобы строка пережила падение."""
    with conn.cursor() as cur:
        cur.execute(
            f"insert into {schema}.etl_load_log (source, ds, status) values (%s, %s, 'running') returning id",
            (source, ds),
        )
        log_id = cur.fetchone()[0]
    conn.commit()
    return log_id


def log_finish(schema, log_id, status, rows=None, error=None):
    """Закрыть строку журнала. Своё подключение: основное могло упасть."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"update {schema}.etl_load_log set finished_at = now(), status = %s, "
                "rows_loaded = %s, error = %s where id = %s",
                (status, rows, error, log_id),
            )
        conn.commit()
    finally:
        conn.close()


def write_rows(conn, schema, table, cols, rows, ds, source_file, mode):
    """Записать строки в stg ОДНОЙ транзакцией. Возвращает (удалено, вставлено).

    mode:
      delete_ds - удалить строки с этим _ds, вставить заново (события, API);
      truncate  - очистить всю таблицу, вставить заново (справочники).
    Пустое значение в файле становится NULL.
    """
    import csv
    import io

    tbl = f"{schema}.{table}"
    buf = io.StringIO()
    w = csv.writer(buf)
    for r in rows:
        w.writerow(list(r) + [ds.isoformat(), source_file])
    buf.seek(0)
    collist = ", ".join(cols + ["_ds", "_source_file"])
    with conn.cursor() as cur:
        if mode == "delete_ds":
            cur.execute(f"delete from {tbl} where _ds = %s", (ds,))
        elif mode == "truncate":
            cur.execute(f"delete from {tbl}")
        else:
            raise ValueError(f"неизвестный режим {mode!r}")
        n_del = cur.rowcount
        if rows:
            cur.copy_expert(f"copy {tbl} ({collist}) from stdin with (format csv)", buf)
        n_ins = len(rows)
    conn.commit()
    return n_del, n_ins
