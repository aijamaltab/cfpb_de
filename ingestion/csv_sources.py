"""CSV-источники финального DAG: событие с датой (ставка ФРС, FRED DFF) и два справочника.

Слои те же, что у API: файл -> stg_* (всё text + _ds, _source_file, _loaded_at) -> ods_* (типы, ключ).
События грузятся за один день ds (delete + insert по _ds), справочники целиком (truncate + insert).
Громкие ошибки: нет файла, не тот заголовок, в файле нет данных за ds, дубль дня, неизвестный тип компании.
Пароля здесь нет: подключение берёт get_conn() из cfpb_pipeline (Airflow Connection или PGPASSWORD).
"""
import csv
import os
import subprocess
from datetime import date, datetime
from pathlib import Path

import common
from cfpb_pipeline import get_conn, write_log, run_ods_step, STG, ODS

DATA = Path(__file__).resolve().parent.parent / "data"
FRED_FILE = DATA / "fred" / "DFF.csv"
DIM_COMPANY_FILE = DATA / "ref" / "dim_company.csv"
COMPANY_BANK_FILE = DATA / "ref" / "company_bank_map.csv"

FRED_URL = os.environ.get("FRED_URL", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFF&cosd=2023-10-01")
FRED_HEADER = ["observation_date", "dff"]
DIM_COMPANY_HEADER = ["company_cfpb", "canonical_name", "company_type", "fdic_candidate", "note"]
COMPANY_BANK_HEADER = ["company_cfpb", "cert", "bank_name", "note"]

# допустимые типы компании (проверяется при загрузке справочника и в dbt-тесте)
COMPANY_TYPES = [
    "auto_lender", "bank", "big_bank", "brokerage_fintech", "card_issuer", "consumer_lender", "credit_bureau",
    "credit_union", "crypto_exchange", "fintech_lending", "fintech_neobank", "fintech_payments", "mortgage_lender",
    "mortgage_servicer", "prepaid_cards", "regional_bank", "student_loan_servicer", "unknown",
]


def read_csv(path, expected_header):
    """Файл целиком: список строк-списков. Заголовок сверяется (без учёта регистра и BOM)."""
    if not Path(path).is_file():
        raise RuntimeError(f"нет файла {path} (см. README, шаг «CSV-данные»)")
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        rows = [r for r in reader if r]
    if header is None or [h.strip().lower() for h in header] != expected_header:
        raise RuntimeError(f"{Path(path).name}: заголовок {header}, ожидали {expected_header}")
    for i, r in enumerate(rows, start=2):
        if len(r) != len(expected_header):
            raise RuntimeError(f"{Path(path).name}, строка {i}: {len(r)} полей вместо {len(expected_header)}")
    return rows


def _stg_columns(conn, table):
    with conn.cursor() as cur:
        cols = common.table_columns(cur, STG, table)
    conn.rollback()
    if not cols:
        raise RuntimeError(f"таблицы {STG}.{table} нет: выполните sql/31_csv_tables.sql")
    return cols


# ---------- событие с датой: FRED DFF ----------

def refresh_fred_file():
    """Файл не дотягивает до даты ds (запуск по расписанию идёт за сегодняшние дни): докачать ряд целиком, без ключа.
    Качаем во временный файл, проверяем заголовок и размер, потом подменяем основной. Нет сети или прав на запись:
    понятная ошибка, файл остаётся прежним (его можно обновить командой curl из README)."""
    tmp = FRED_FILE.with_name(FRED_FILE.name + ".tmp")
    try:
        p = subprocess.run(["curl", "-sS", "-f", "-m", "60", "-o", str(tmp), FRED_URL], capture_output=True, text=True)
        if p.returncode != 0:
            raise RuntimeError(f"не удалось скачать ряд FRED (curl код {p.returncode}): {p.stderr.strip()[:150]}")
        rows = read_csv(tmp, FRED_HEADER)
        if len(rows) < 1000:
            raise RuntimeError(f"скачанный ряд FRED короткий ({len(rows)} строк), файл не заменён")
        tmp.replace(FRED_FILE)
    except OSError as error:
        raise RuntimeError(f"не удалось обновить {FRED_FILE.name}: {error}. Обновите файл командой curl из README") from error
    finally:
        tmp.unlink(missing_ok=True)
    return rows


MAX_FRED_LAG_DAYS = 14          # на сколько дней ряд FRED может отставать от ds, чтобы ods ещё переносил последнюю ставку


def do_load_stg_fred(ds):
    """Строка ряда FRED за день ds -> stg_fred_dff (как в файле, всё text). Ряд выходит с задержкой в 1-3 дня,
    поэтому за самые свежие дни строки ещё нет: тогда в stg пусто (stg хранит только то, что пришло), а ods
    переносит последнюю известную ставку (do_load_ods_fred). Дыра внутри ряда и дата раньше начала файла: ошибка."""
    day = date.fromisoformat(ds)
    started_at = datetime.now()
    conn = get_conn()
    try:
        cols = _stg_columns(conn, "stg_fred_dff")
        rows = read_csv(FRED_FILE, FRED_HEADER)
        dates = [r[0] for r in rows]
        if not dates:
            raise RuntimeError(f"{FRED_FILE.name}: файл пустой")
        if ds > max(dates):                    # файл не дотягивает до ds: пробуем докачать ряд
            try:
                rows = refresh_fred_file()
                dates = [r[0] for r in rows]
            except Exception as error:         # noqa: BLE001 - нет сети или прав на запись (воркер Airflow): не страшно, ods перенесёт ставку
                print(f"{ds}: ряд FRED не обновлён ({error})")
        if ds < min(dates):
            raise RuntimeError(f"{FRED_FILE.name} покрывает {min(dates)} .. {max(dates)}, за {ds} данных нет: обновите файл (README, шаг «CSV-данные»)")
        day_rows = [r for r in rows if r[0] == ds]
        if not day_rows:
            lag = (day - date.fromisoformat(max(dates))).days
            if lag < 0:
                raise RuntimeError(f"в {FRED_FILE.name} нет строки за {ds}")             # дыра внутри ряда
            if lag > MAX_FRED_LAG_DAYS:
                raise RuntimeError(f"{FRED_FILE.name} кончается {max(dates)}, это на {lag} дн. раньше {ds} (допустимо {MAX_FRED_LAG_DAYS}): обновите файл")
            print(f"{ds}: в FRED за эту дату ещё нет строки (последняя {max(dates)}); в stg пусто, ods перенесёт последнюю ставку")
        if len(day_rows) > 1:
            raise RuntimeError(f"в {FRED_FILE.name} за {ds} строк {len(day_rows)}, ожидали одну")
        n_del, n_ins = common.write_rows(conn, STG, "stg_fred_dff", cols, day_rows, day, FRED_FILE.name, "delete_ds")
    except Exception as error:
        conn.rollback()
        conn.close()
        write_log("stg_fred_dff", ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    write_log("stg_fred_dff", ds, started_at, "success", n_ins, None)
    return n_ins


ODS_FRED_DELETE = "delete from " + ODS + ".ods_fred_dff where ds = %(ds)s"
ODS_FRED_INSERT = """
    insert into """ + ODS + """.ods_fred_dff (ds, fed_funds_rate)
    select distinct on (observation_date)
           observation_date::date,
           nullif(nullif(trim(dff), ''), '.')::numeric
    from """ + STG + """.stg_fred_dff
    where _ds = %(ds)s
    order by observation_date, _loaded_at desc
"""


# Ставка за ds ещё не опубликована: берём последнюю известную (эффективная ставка действует, пока её не изменят).
# Строка помечается is_carried = true; при повторном запуске даты после выхода ряда FRED подставится настоящее значение.
ODS_FRED_CARRY = """
    insert into """ + ODS + """.ods_fred_dff (ds, fed_funds_rate, is_carried)
    select %(ds)s::date, fed_funds_rate, true
    from """ + ODS + """.ods_fred_dff
    where ds < %(ds)s::date and fed_funds_rate is not null
    order by ds desc
    limit 1
"""


def do_load_ods_fred(ds):
    """stg -> ods: ровно одна строка на день ds. Нет строки в stg (ряд FRED отстаёт): переносим последнюю ставку."""
    started_at = datetime.now()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(ODS_FRED_DELETE, {"ds": ds})
        cur.execute(ODS_FRED_INSERT, {"ds": ds})
        rows = cur.rowcount
        carried = rows == 0
        if carried:
            cur.execute(ODS_FRED_CARRY, {"ds": ds})
            rows = cur.rowcount
        conn.commit()
    except Exception as error:
        conn.rollback()
        conn.close()
        write_log("ods_fred_dff", ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    if rows != 1:
        write_log("ods_fred_dff", ds, started_at, "failed", rows, f"строк {rows}, ожидали 1 (нет данных ни за {ds}, ни за прошлые дни)")
        raise RuntimeError(f"ods_fred_dff за {ds}: строк {rows}, ожидали 1")
    print(f"{ds}: ставка ФРС " + ("перенесена с последнего известного дня (is_carried = true)" if carried else "из файла"))
    write_log("ods_fred_dff", ds, started_at, "success", rows, None)
    return rows


# ---------- справочники: целиком ----------

def do_load_stg_refs(ds):
    """dim_company.csv и company_bank_map.csv целиком в stg (truncate + insert)."""
    day = date.fromisoformat(ds)
    started_at = datetime.now()
    conn = get_conn()
    counts = {}
    try:
        for table, path, header in (("stg_dim_company", DIM_COMPANY_FILE, DIM_COMPANY_HEADER),
                                    ("stg_company_bank_map", COMPANY_BANK_FILE, COMPANY_BANK_HEADER)):
            cols = _stg_columns(conn, table)
            rows = read_csv(path, header)
            if not rows:
                raise RuntimeError(f"{path.name}: справочник пустой")
            # NULL вместо пустой строки ставит write_rows (csv-формат COPY)
            _, counts[table] = common.write_rows(conn, STG, table, cols, rows, day, path.name, "truncate")
    except Exception as error:
        conn.rollback()
        conn.close()
        write_log("stg_refs", ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    for table, n in counts.items():
        write_log(table, ds, started_at, "success", n, None)
    return counts


def do_load_ods_refs(ds):
    """Справочники stg -> ods: типы, ключи, проверка значений. Одна транзакция, повтор безопасен."""
    started_at = datetime.now()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("select distinct company_type from " + STG + ".stg_dim_company where company_type <> all(%(ok)s)",
                    {"ok": COMPANY_TYPES})
        bad = [r[0] for r in cur.fetchall()]
        if bad:
            raise RuntimeError(f"dim_company: неизвестный тип компании {bad}")
        cur.execute("delete from " + ODS + ".ods_dim_company")
        cur.execute(
            "insert into " + ODS + ".ods_dim_company (company_cfpb, canonical_name, company_type, fdic_candidate, note) "
            "select company_cfpb, canonical_name, company_type, "
            "case fdic_candidate when 'yes' then true when 'no' then false end, nullif(note, '') "
            "from " + STG + ".stg_dim_company")
        n_company = cur.rowcount
        cur.execute("delete from " + ODS + ".ods_company_bank")
        cur.execute(
            "insert into " + ODS + ".ods_company_bank (company_cfpb, cert, bank_name, note) "
            "select company_cfpb, cert::bigint, bank_name, nullif(note, '') from " + STG + ".stg_company_bank_map")
        n_map = cur.rowcount
        # каждая компания из привязки к банку должна быть в справочнике компаний
        cur.execute(
            "select m.company_cfpb from " + ODS + ".ods_company_bank m "
            "left join " + ODS + ".ods_dim_company c on c.company_cfpb = m.company_cfpb where c.company_cfpb is null")
        orphans = [r[0] for r in cur.fetchall()]
        if orphans:
            raise RuntimeError(f"company_bank_map: компании нет в dim_company: {orphans[:3]}")
        conn.commit()
    except Exception as error:
        conn.rollback()
        conn.close()
        write_log("ods_refs", ds, started_at, "failed", 0, str(error)[:500])
        raise
    conn.close()
    write_log("ods_dim_company", ds, started_at, "success", n_company, None)
    write_log("ods_company_bank", ds, started_at, "success", n_map, None)
    return {"dim_company": n_company, "company_bank": n_map}
