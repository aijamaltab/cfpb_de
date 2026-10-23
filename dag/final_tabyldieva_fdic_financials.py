"""DAG: квартальные показатели банков США (FDIC /financials) -> stg -> ods -> проверки.

Каждый запуск перезагружает все кварталы окна с 2023 года (около 4,5 тыс. банков на квартал, меньше лимита 100 тыс. на запуск).
Раз в месяц, catchup выключен. Первый запуск: кнопка Trigger DAG.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import __version__ as AF_VERSION
from airflow.decorators import task
from _lib.tenant import student_dag

sys.path.insert(0, str(Path(__file__).parent / "final_project" / "ingestion"))

import fdic_fin_pipeline as fp  # noqa: E402

_major, _minor = (int(x) for x in AF_VERSION.split(".")[:2])
SCHEDULE = {"schedule": "@monthly"} if (_major, _minor) >= (2, 4) else {"schedule_interval": "@monthly"}

with student_dag(
    __file__,
    dag_id="fdic_financials_pipeline",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["final_project", "fdic", "financials"],
    **SCHEDULE,
) as dag:

    @task
    def load_stg(ds=None):
        return fp.do_load_stg(ds)

    @task
    def load_ods(ds=None):
        return fp.do_load_ods(ds)

    @task
    def dq_check(ds=None):
        fp.do_dq_check(ds)

    load_stg() >> load_ods() >> dq_check()