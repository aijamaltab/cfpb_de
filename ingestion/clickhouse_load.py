"""Загрузка из PostgreSQL в ClickHouse (HTTP-интерфейс, без доп. библиотек): копия ods и витрины (dm_complaints_daily,
dm_company_month, dm_bank_size_quarter, а также dbt-витрины dm_state_month, dm_type_daily и dm_open_cases).
"""
import json
import os
import urllib.parse
import urllib.request
from datetime import date, datetime

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


# ---------- остальные витрины: те же таблицы и те же суммы в ClickHouse, без дублей ----------
# Описание витрины: cols - колонки как в PostgreSQL и ClickHouse (по порядку); key - сортировка и ключ строки;
# dates - колонки-даты; nums - numeric (в JSON уходят числом); sums - целые метрики, по которым сверяются обе базы.
MARTS = {
    "dm_company_month": {          # DAG final_tabyldieva_datamart, схема <логин>_datamart
        "cols": ["month", "company", "company_type", "complaints_cnt", "untimely_cnt", "relief_cnt"],
        "key": ["month", "company"], "dates": ["month"], "nums": [],
        "sums": ["complaints_cnt", "untimely_cnt", "relief_cnt"],
    },
    "dm_bank_size_quarter": {      # DAG final_tabyldieva_datamart, схема <логин>_datamart
        "cols": ["quarter_start", "company", "banks_cnt", "complaints_cnt", "assets_k_usd", "deposits_k_usd",
                 "complaints_per_bn_assets", "complaints_per_bn_deposits"],
        "key": ["quarter_start", "company"], "dates": ["quarter_start"],
        "nums": ["complaints_per_bn_assets", "complaints_per_bn_deposits"],
        "sums": ["complaints_cnt", "assets_k_usd"],
    },
    "dm_state_month": {            # dbt, схема <логин>_dbt
        "cols": ["month", "state", "state_name", "complaints_cnt", "untimely_cnt", "relief_cnt"],
        "key": ["month", "state"], "dates": ["month"], "nums": [],
        "sums": ["complaints_cnt", "untimely_cnt", "relief_cnt"],
    },
    "dm_open_cases": {             # dbt, слой CDC (tag:cdc), схема <логин>_dbt; снимок «на сейчас»
        "cols": ["product", "status", "status_group", "cases_cnt", "avg_age_days"],
        "key": ["product", "status"], "dates": [], "nums": ["avg_age_days"], "sums": ["cases_cnt"],
    },
    "dm_type_daily": {             # dbt, схема <логин>_dbt
        "cols": ["ds", "company_type", "is_weekend", "complaints_cnt", "untimely_cnt", "relief_cnt", "fed_funds_rate"],
        "key": ["ds", "company_type"], "dates": ["ds"], "nums": ["fed_funds_rate"],
        "sums": ["complaints_cnt", "untimely_cnt", "relief_cnt"],
    },
    "dq_metrics": {                # DAG final_tabyldieva_dq, схема <логин>_datamart; история метрик качества по дням
        "cols": ["check_date", "dimension", "layer", "table_name", "metric", "title", "value", "threshold", "unit",
                 "severity", "status", "failed"],
        "key": ["check_date", "table_name", "metric"], "dates": ["check_date"], "nums": ["value", "threshold"],
        "sums": ["failed"],
    },
}


def _mart_json(pg_cur, schema, name, where_sql="", params=None):
    """Строки витрины из PostgreSQL как JSONEachRow и суммы метрик для сверки: (строки, {колонка: сумма})."""
    spec = MARTS[name]
    pg_cur.execute("select " + ", ".join(spec["cols"]) + " from " + schema + "." + name + " " + where_sql
                   + " order by " + ", ".join(spec["key"]), params or {})
    rows, sums = [], {c: 0 for c in spec["sums"]}
    for rec in pg_cur.fetchall():
        d = dict(zip(spec["cols"], rec))
        for c in spec["dates"]:
            d[c] = d[c].isoformat()
        for c in spec["nums"]:
            d[c] = None if d[c] is None else float(d[c])
        for c in spec["sums"]:
            sums[c] += int(d[c])
        rows.append(json.dumps(d, ensure_ascii=False))
    return rows, sums


def _check_mart_generic(table, where_ch, n_rows, sums, label):
    """Число строк и суммы метрик в ClickHouse (FINAL) равны PostgreSQL, иначе ошибка и красная задача."""
    cols = list(sums)
    out = ch("SELECT count(), " + ", ".join(f"sum({c})" for c in cols) + f" FROM {table} FINAL WHERE {where_ch}")
    got = [int(x) for x in out.split("\t")]
    want = [n_rows] + [sums[c] for c in cols]
    if got != want:
        raise RuntimeError(f"ClickHouse не сошёлся ({label}): PostgreSQL строк {n_rows}, суммы "
                           + ", ".join(f"{c}={sums[c]}" for c in cols)
                           + f"; ClickHouse строк {got[0]}, суммы " + ", ".join(f"{c}={v}" for c, v in zip(cols, got[1:])))


def load_mart_slice(pg_cur, schema, name, pg_where, pg_params, ch_where, label):
    """Перегрузить срез витрины (удалить в ClickHouse, вставить из PostgreSQL, сверить). Возвращает число строк."""
    rows, sums = _mart_json(pg_cur, schema, name, pg_where, pg_params)
    table = f"{setting('db', 'default')}.{name}"
    ch(f"ALTER TABLE {table} DELETE WHERE {ch_where} SETTINGS mutations_sync = 2")
    if rows:
        ch(f"INSERT INTO {table} FORMAT JSONEachRow", ("\n".join(rows) + "\n").encode(), timeout=180)
    _check_mart_generic(table, ch_where, len(rows), sums, label)
    return len(rows)


def load_mart_full(pg_cur, schema, name):
    """Залить витрину целиком (TRUNCATE + вставка пачками + сверка). Для первичной заливки, пересборки и dbt-витрин."""
    rows, sums = _mart_json(pg_cur, schema, name)
    table = f"{setting('db', 'default')}.{name}"
    ch(f"TRUNCATE TABLE {table}")
    for i in range(0, len(rows), 5000):
        ch(f"INSERT INTO {table} FORMAT JSONEachRow", ("\n".join(rows[i:i + 5000]) + "\n").encode(), timeout=180)
    _check_mart_generic(table, "1", len(rows), sums, f"{name}, вся таблица")
    return len(rows)


def load_company_month_day(pg_cur, dm_schema, ds):
    """dm_company_month: перегрузить месяц дня ds (в PostgreSQL DAG пересчитывает тот же месяц)."""
    month = date.fromisoformat(ds).replace(day=1)
    return load_mart_slice(pg_cur, dm_schema, "dm_company_month", "where month = %(m)s", {"m": month},
                           f"month = '{month.isoformat()}'", f"dm_company_month {month.isoformat()}")


def load_bank_size(pg_cur, dm_schema):
    """dm_bank_size_quarter: в PostgreSQL витрина пересобирается целиком, в ClickHouse тоже."""
    return load_mart_full(pg_cur, dm_schema, "dm_bank_size_quarter")
