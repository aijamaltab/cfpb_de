"""final_tabyldieva_dbt_marts: витрины dbt (dm_state_month, dm_type_daily) раз в сутки и их копия в ClickHouse.

  dbt_build_marts           dbt build --select dm_state_month dm_type_daily (модели и их тесты); нет dbt у воркера: пропуск
  load_clickhouse_dbt_marts обе витрины целиком в ClickHouse tabyldieva.*, сверка строк и сумм (при tabyldieva_ch_enabled = 1)
Зачем отдельный DAG: обязательные DAG (загрузка и витрины по ФТТ) не зависят от dbt и остаются зелёными, если dbt недоступен.
Создаётся на паузе; включать после того, как таблицы в ClickHouse созданы (sql/35). Это дополнение сверх минимума ФТТ.
Подключение к PostgreSQL: Airflow Connection aiacademy_pg_tabyldieva (пароля в коде нет).
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import task

# Рабочая копия лежит в ~/work/dags/ (так её видит Airflow стенда), исходник в final_project/dag/: ищем ingestion там и там.
_HERE = Path(__file__).resolve().parent
for _p in (_HERE.parent / "ingestion", _HERE / "final_project" / "ingestion"):
    if (_p / "cfpb_pipeline.py").is_file():
        sys.path.insert(0, str(_p))
        break

import dbt_marts_pipeline as dm      # noqa: E402

DAG_ID = "final_tabyldieva_dbt_marts"

try:
    from _lib.tenant import student_dag      # общий стенд курса: добавляет к dag_id префикс <логин>.
except ModuleNotFoundError as _e:            # вне стенда (чужая установка, локальная проверка): обычный DAG
    if _e.name not in ("_lib", "_lib.tenant"):
        raise
    student_dag = None


def make_dag(**kwargs):
    if student_dag is not None:
        return student_dag(__file__, **kwargs)
    from airflow import DAG
    return DAG(**kwargs)


with make_dag(
    dag_id=DAG_ID,
    start_date=datetime(2026, 10, 1),
    schedule="0 2 * * *",                    # 02:00 UTC: после ночного запуска основного DAG (он идёт около двух минут)
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["final_project", "dbt", "tabyldieva"],
    doc_md=__doc__,
) as dag:
    @task
    def dbt_build_marts(ds=None):
        return dm.do_dbt_build_marts(ds)

    @task
    def load_clickhouse_dbt_marts(ds=None):
        return dm.do_load_clickhouse_dbt_marts(ds)

    dbt_build_marts() >> load_clickhouse_dbt_marts()
