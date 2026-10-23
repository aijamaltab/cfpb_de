"""Загрузка из PostgreSQL в ClickHouse (HTTP-интерфейс, без доп. библиотек): копия ods и витрина dm_complaints_daily.

Режим как везде: delete + insert по ds, потом сверка числа строк (и суммы метрики для витрины). Повтор безопасен.
Параметры подключения: Airflow Variables tabyldieva_ch_host / port / user / password / db (или переменные окружения CH_*).
Проверено на настоящем ClickHouse: число строк равно ods по обоим контурам (банки и бюро).
"""
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime

CH_TABLE = "cfpb_complaints"


def setting(name, default=""):
    """Параметр подключения: сначала переменная окружения CH_<NAME>, затем Airflow Variable tabyldieva_ch_<name>.
    Читается в момент вызова (не при разборе DAG). Паролей в коде нет."""
    value = os.environ.get("CH_" + name.upper())
    if value:
        return value
    try:
        from airflow.models import Variable
        return Variable.get("tabyldieva_ch_" + name, default_var=default)
    except Exception:                                   # noqa: BLE001 - вне Airflow Variable недоступна
        return default


def ch(query, data=None, timeout=60):
    """Один запрос к ClickHouse. Без data: запрос в теле. С data: INSERT ... FORMAT, строки в теле."""
    url = f"http://{setting('host', 'clickhouse')}:{setting('port', '8123')}/"
    headers = {"X-ClickHouse-User": setting("user", "default"), "X-ClickHouse-Key": setting("password")}
    if data is None:
        req = urllib.request.Request(url, data=query.encode(), headers=headers, method="POST")
    else:
        req = urllib.request.Request(url + "?" + urllib.parse.urlencode({"query": query}), data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode().strip()


def load_day(pg_cur, ods_schema, ds, ods_table="ods_complaints", ch_table=CH_TABLE):
    """Перегрузить день ds. Возвращает число строк. Бросает ошибку, если после загрузки счёт не сошёлся."""
    pg_cur.execute(
        "select complaint_id, received_at, ds, product, coalesce(sub_product, ''), coalesce(issue, ''), company, "
        "coalesce(state, ''), coalesce(company_response, ''), case when timely then 1 else 0 end, coalesce(submitted_via, '') "
        "from " + ods_schema + "." + ods_table + " where ds = %(ds)s order by complaint_id", {"ds": ds})
    keys = ["complaint_id", "received_at", "ds", "product", "sub_product", "issue", "company",
            "state", "company_response", "timely", "submitted_via"]
    rows = []
    for rec in pg_cur.fetchall():
        d = dict(zip(keys, rec))
        d["received_at"] = d["received_at"].strftime("%Y-%m-%d %H:%M:%S")
        d["ds"] = d["ds"].isoformat()
        rows.append(json.dumps(d, ensure_ascii=False))
    table = f"{setting('db', 'default')}.{ch_table}"
    ch(f"ALTER TABLE {table} DELETE WHERE ds = '{ds}' SETTINGS mutations_sync = 2")
    if rows:
        ch(f"INSERT INTO {table} FORMAT JSONEachRow", ("\n".join(rows) + "\n").encode(), timeout=180)
    in_ch = int(ch(f"SELECT count() FROM {table} FINAL WHERE ds = '{ds}'") or 0)
    if in_ch != len(rows):
        raise RuntimeError(f"ClickHouse не сошёлся за {ds}: ods {len(rows)}, ClickHouse {in_ch}")
    return len(rows)


# ---------- витрина dm_complaints_daily (ФТ-7: та же таблица, тот же день, без дублей) ----------

MART_TABLE = "dm_complaints_daily"
MART_COLS = ["ds", "product", "complaints_cnt", "timely_cnt", "untimely_cnt", "in_progress_cnt", "relief_cnt", "fed_funds_rate"]


def _mart_rows(pg_cur, dm_schema, where_sql, params):
    pg_cur.execute("select " + ", ".join(MART_COLS) + " from " + dm_schema + "." + MART_TABLE + " " + where_sql
                   + " order by ds, product", params)
    rows, total = [], 0
    for rec in pg_cur.fetchall():
        d = dict(zip(MART_COLS, rec))
        d["ds"] = d["ds"].isoformat()
        d["fed_funds_rate"] = None if d["fed_funds_rate"] is None else float(d["fed_funds_rate"])
        total += d["complaints_cnt"]
        rows.append(json.dumps(d, ensure_ascii=False))
    return rows, total


def _check_mart(table, where_ch, n_rows, pg_sum, label):
    in_ch = int(ch(f"SELECT count() FROM {table} FINAL WHERE {where_ch}") or 0)
    ch_sum = int(ch(f"SELECT sum(complaints_cnt) FROM {table} FINAL WHERE {where_ch}") or 0)
    if in_ch != n_rows or ch_sum != pg_sum:
        raise RuntimeError(f"ClickHouse не сошёлся ({label}): PostgreSQL {n_rows} строк / сумма {pg_sum}, "
                           f"ClickHouse {in_ch} строк / сумма {ch_sum}")


def load_mart_day(pg_cur, dm_schema, ds):
    """Перегрузить день ds витрины. Возвращает число строк. Ошибка, если число строк или сумма метрики не сошлись."""
    rows, pg_sum = _mart_rows(pg_cur, dm_schema, "where ds = %(ds)s", {"ds": ds})
    table = f"{setting('db', 'default')}.{MART_TABLE}"
    ch(f"ALTER TABLE {table} DELETE WHERE ds = '{ds}' SETTINGS mutations_sync = 2")
    if rows:
        ch(f"INSERT INTO {table} FORMAT JSONEachRow", ("\n".join(rows) + "\n").encode(), timeout=180)
    _check_mart(table, f"ds = '{ds}'", len(rows), pg_sum, ds)
    return len(rows)


def load_mart_all(pg_cur, dm_schema):
    """Первичная заливка всей истории витрины (один раз после создания таблицы). Возвращает число строк."""
    rows, pg_sum = _mart_rows(pg_cur, dm_schema, "", {})
    table = f"{setting('db', 'default')}.{MART_TABLE}"
    ch(f"TRUNCATE TABLE {table}")
    for i in range(0, len(rows), 5000):
        ch(f"INSERT INTO {table} FORMAT JSONEachRow", ("\n".join(rows[i:i + 5000]) + "\n").encode(), timeout=180)
    _check_mart(table, "1", len(rows), pg_sum, "вся история")
    return len(rows)
