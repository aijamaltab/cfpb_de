"""final_tabyldieva_dq: метрики качества данных по шести измерениям, раз в сутки, история в dq_metrics.

  compute_dq_metrics   считает метрики (полнота, уникальность, валидность, согласованность, точность, актуальность)
                       по слоям stg / ods / datamart / dbt / CDC и пишет их в <логин>_datamart.dq_metrics за дату запуска
  load_clickhouse_dq   таблица dq_metrics целиком в ClickHouse tabyldieva.dq_metrics (для Superset), сверка строк и числа fail
  check_thresholds     красная задача, если есть метрика со статусом fail (severity = error); warn только в логе
Сначала запись и копия, потом проверка порогов: даже при плохом качестве метрики видны на дашборде.
Время 03:00 UTC: после ночной загрузки (около 00:00), перезагрузки окна (00:30) и витрин dbt (02:00).
Формулы и пороги: ingestion/dq_metrics.py. Подключение к PostgreSQL: Airflow Connection aiacademy_pg_tabyldieva.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import task

_HERE = Path(__file__).resolve().parent
for _p in (_HERE.parent / "ingestion", _HERE / "final_project" / "ingestion"):
    if (_p / "cfpb_pipeline.py").is_file():
        sys.path.insert(0, str(_p))
        break

import dq_metrics as dq      # noqa: E402

DAG_ID = "final_tabyldieva_dq"

try:
    from _lib.tenant import student_dag      # общий стенд курса: добавляет к dag_id префикс <логин>.
except ModuleNotFoundError as _e:
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
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["final_project", "dq", "tabyldieva"],
    doc_md=__doc__,
) as dag:
    @task
    def compute_dq_metrics(ds=None):
        return dq.do_compute(ds)

    @task
    def load_clickhouse_dq(ds=None):
        return dq.do_load_clickhouse(ds)

    @task(trigger_rule="all_done")           # пороги проверяем, даже если ClickHouse выключен (задача копии пропущена)
    def check_thresholds(ds=None):
        return dq.check(ds)

    m = compute_dq_metrics()
    m >> load_clickhouse_dq() >> check_thresholds()
