"""dbt-витрины и их копия в ClickHouse: dm_state_month, dm_type_daily (DAG final_tabyldieva_dbt_marts, раз в сутки)
и dm_open_cases (слой CDC, раз в час в DAG final_tabyldieva_cdc_integration).

Зачем отдельно от DAG витрин: DAG витрин по ФТТ считает витрины из ods в Postgres, а эти витрины строит dbt. Если dbt недоступен
воркеру или упадёт, обязательные DAG (загрузка, витрины) остаются зелёными. Это дополнение сверх минимума ФТТ.
Подключение dbt берётся так же, как в слое CDC (cdc_integration._dbt_env): пароль из Airflow Connection, в файлах его нет.
"""
import shutil
import subprocess
from datetime import datetime

import cdc_integration as ci
import cfpb_pipeline as cp

DBT_SCHEMA = cp.LOGIN + "_dbt"
DBT_MARTS = ["dm_state_month", "dm_type_daily"]


def do_dbt_build_marts(ds):
    """dbt build --select dm_state_month dm_type_daily: модели и их тесты. Нет dbt у воркера -> пропуск, а не ошибка."""
    from airflow.exceptions import AirflowSkipException
    if not shutil.which("dbt"):
        raise AirflowSkipException("dbt недоступен воркеру Airflow: запускайте руками (cd dbt/tabyldieva_dwh && dbt build -s dm_state_month dm_type_daily)")
    started_at = datetime.now()
    result = subprocess.run(["dbt", "build", "--select", *DBT_MARTS], cwd=str(ci.DBT_DIR), env=ci._dbt_env(),
                            capture_output=True, text=True, timeout=900)
    tail = "\n".join((result.stdout or "").strip().splitlines()[-12:])
    print(tail)
    if result.returncode != 0:
        cp.write_log("dbt_build_marts", ds, started_at, "failed", 0, tail[-500:])
        raise RuntimeError("dbt build витрин упал, см. лог задачи")
    cp.write_log("dbt_build_marts", ds, started_at, "success", 0, None)


def _copy_to_clickhouse(names, ds):
    """Залить витрины dbt целиком в ClickHouse; по строке журнала на каждую витрину. Первая ошибка останавливает задачу."""
    import clickhouse_load
    for name in names:
        started_at = datetime.now()
        conn = cp.get_conn()
        try:
            with conn.cursor() as cur:
                rows = clickhouse_load.load_mart_full(cur, DBT_SCHEMA, name)
        except Exception as error:
            conn.close()
            cp.write_log("clickhouse_" + name, ds, started_at, "failed", 0, str(error)[:500])
            raise
        conn.close()
        cp.write_log("clickhouse_" + name, ds, started_at, "success", rows, None)


def _skip_if_off():
    from airflow.exceptions import AirflowSkipException
    if not cp.clickhouse_enabled():
        raise AirflowSkipException("ClickHouse выключен (tabyldieva_ch_enabled != 1)")


def do_load_clickhouse_dbt_marts(ds):
    """dm_state_month, dm_type_daily -> ClickHouse (целиком), сверка строк и сумм."""
    _skip_if_off()
    _copy_to_clickhouse(DBT_MARTS, ds)


def do_load_clickhouse_open_cases(ds):
    """dm_open_cases (CDC, снимок «на сейчас») -> ClickHouse (целиком), сверка строк и суммы cases_cnt."""
    _skip_if_off()
    _copy_to_clickhouse(["dm_open_cases"], ds)
