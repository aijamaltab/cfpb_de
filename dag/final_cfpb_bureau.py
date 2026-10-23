"""DAG кредитных бюро и коллекторов CFPB: API -> stg -> сверка -> ods -> проверки -> ClickHouse.

Отдельный от основного контур (cfpb_complaints_pipeline). Один запуск = один день, около 12 тыс. строк
(лимит курса 100 тыс. на запуск). Окно: 1 июля - 30 сентября 2026. Агрегации на объединённых данных считает Spark.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import __version__ as AF_VERSION
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from _lib.tenant import student_dag

sys.path.insert(0, str(Path(__file__).parent / "final_project" / "ingestion"))

import bureau_pipeline as bp  # noqa: E402
import cfpb_pipeline  # noqa: E402  (clickhouse_enabled)

_major, _minor = (int(x) for x in AF_VERSION.split(".")[:2])
SCHEDULE = {"schedule": "@daily"} if (_major, _minor) >= (2, 4) else {"schedule_interval": "@daily"}

with student_dag(
    __file__,
    dag_id="cfpb_bureau_pipeline",
    start_date=datetime(2026, 7, 1),
    end_date=datetime(2026, 9, 30),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["final_project", "cfpb", "bureau"],
    **SCHEDULE,
) as dag:

    @task
    def load_stg(ds=None):
        return bp.do_load_stg(ds)

    @task
    def reconcile(ds=None):
        bp.do_reconcile(ds)

    @task
    def load_ods(ds=None):
        return bp.do_load_ods(ds)

    @task
    def dq_check(ds=None):
        bp.do_dq_check(ds)

    @task
    def load_clickhouse(ds=None):
        if not cfpb_pipeline.clickhouse_enabled():
            raise AirflowSkipException("ClickHouse выключен (Variable tabyldieva_ch_enabled != 1)")
        return bp.do_load_clickhouse(ds)

    load_stg() >> reconcile() >> load_ods() >> dq_check() >> load_clickhouse()
