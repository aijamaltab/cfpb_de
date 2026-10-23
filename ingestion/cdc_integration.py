"""Контур CDC в общем проекте: проверка приёмника и пересборка в dbt только слоя CDC (модели с меткой cdc).

Нужен отдельным DAG, а не задачей батчевого: у контуров разная скорость. Батч идёт раз в сутки, поток живёт постоянно.
Если упадёт батч, слой CDC продолжает обновляться, и наоборот. Функции вызывает dag/final_tabyldieva_cdc_integration.py.
"""
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

import cfpb_pipeline as cp

SINK = cp.ODS + ".ods_complaint_cases"
MAX_LAG_MINUTES = 60          # отставание приёмника больше этого -> предупреждение (не ошибка: источник синтетический)
DBT_DIR = Path(__file__).resolve().parent.parent / "dbt" / "tabyldieva_dwh"


def do_check_cdc_health(ds):
    """Два вопроса к приёмнику: есть ли в нём строки (ошибка, если нет) и давно ли он менялся (только предупреждение).
    Результат пишется в журнал etl_load_log (source = cdc_health)."""
    started_at = datetime.now()
    conn = cp.get_conn()
    cur = conn.cursor()
    try:
        cur.execute("select count(*), coalesce(extract(epoch from now() - max(updated_at)) / 60, -1) from " + SINK)
        rows, lag_minutes = cur.fetchone()
    except Exception as error:      # noqa: BLE001 - таблицы нет: коннектор ещё не создан
        conn.rollback()
        conn.close()
        cp.write_log("cdc_health", ds, started_at, "failed", 0, ("нет приёмника " + SINK + ": " + str(error))[:500])
        raise
    conn.close()
    lag_minutes = float(lag_minutes)
    note = f"отставание {round(lag_minutes)} мин"
    if lag_minutes > MAX_LAG_MINUTES:
        note += f" (больше {MAX_LAG_MINUTES}: предупреждение; данные синтетические, источник давно не менялся)"
    print(f"{ds}: строк в приёмнике {rows}, {note}")
    if rows == 0:
        cp.write_log("cdc_health", ds, started_at, "failed", 0, "приёмник пуст: проверьте коннекторы (connect.sh status)")
        raise RuntimeError("проверка CDC не прошла: в приёмнике нет строк")
    cp.write_log("cdc_health", ds, started_at, "success", rows, None)
    return rows


def _dbt_env():
    """Переменные подключения dbt. Пароль берётся из Airflow Connection (или из PGPASSWORD в терминале), в файлах его нет."""
    env = dict(os.environ)
    if os.environ.get("PGPASSWORD"):
        env.update(DBT_HOST=cp.PG_HOST, DBT_PORT=os.environ.get("PGPORT", "5432"), DBT_USER=os.environ.get("PGUSER", cp.LOGIN),
                   DBT_DBNAME=cp.PG_DB, DBT_PASSWORD=os.environ["PGPASSWORD"])
    else:
        from airflow.hooks.base import BaseHook
        c = BaseHook.get_connection(cp.CONN_ID)
        env.update(DBT_HOST=c.host, DBT_PORT=str(c.port or 5432), DBT_USER=c.login, DBT_DBNAME=c.schema or cp.PG_DB,
                   DBT_PASSWORD=c.password)
    env.update(DBT_PROFILES_DIR=str(DBT_DIR / "profiles"),
               DBT_TARGET_PATH="/tmp/dbt_tabyldieva/target", DBT_LOG_PATH="/tmp/dbt_tabyldieva/logs",   # не мусорим в проекте
               DBT_SEND_ANONYMOUS_USAGE_STATS="False")
    return env


def do_dbt_build_cdc(ds):
    """dbt build --select tag:cdc: пересобирает только слой CDC. Нет dbt у воркера -> задача пропускается, а не падает."""
    from airflow.exceptions import AirflowSkipException
    if not shutil.which("dbt"):
        raise AirflowSkipException("dbt недоступен воркеру Airflow: запускайте руками (cd dbt/tabyldieva_dwh && dbt build -s tag:cdc)")
    started_at = datetime.now()
    result = subprocess.run(["dbt", "build", "--select", "tag:cdc"], cwd=str(DBT_DIR), env=_dbt_env(),
                            capture_output=True, text=True, timeout=600)
    tail = "\n".join((result.stdout or "").strip().splitlines()[-12:])
    print(tail)
    if result.returncode != 0:
        cp.write_log("dbt_build_cdc", ds, started_at, "failed", 0, tail[-500:])
        raise RuntimeError("dbt build --select tag:cdc упал, см. лог задачи")
    cp.write_log("dbt_build_cdc", ds, started_at, "success", 0, None)
