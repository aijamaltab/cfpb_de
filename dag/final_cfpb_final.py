"""DAG жалоб CFPB: API -> stg -> сверка -> ods -> справочники -> витрины dm -> проверки качества -> ClickHouse
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import __version__ as AF_VERSION
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from _lib.tenant import student_dag

sys.path.insert(0, str(Path(__file__).parent / "final_project" / "ingestion"))

import cfpb_pipeline as pipeline 

_major, _minor = (int(x) for x in AF_VERSION.split(".")[:2])
SCHEDULE = {"schedule": "@daily"} if (_major, _minor) >= (2, 4) else {"schedule_interval": "@daily"}

with student_dag(
    __file__,
    dag_id="cfpb_final",
    start_date=datetime(2023, 10, 1),
    end_date=datetime(2026, 9, 30),
    catchup=True,
    max_active_runs=1,                    
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["final_project", "cfpb"],
    **SCHEDULE,
) as dag:

    @task
    def load_stg(ds=None):
        return pipeline.do_load_stg(ds)

    @task
    def reconcile(ds=None):
        pipeline.do_reconcile(ds)

    @task
    def load_ods(ds=None):
        return pipeline.do_load_ods(ds)

    @task
    def load_ref(ds=None):
        return pipeline.do_load_ref(ds)

    @task
    def dq_check(ds=None):
        pipeline.do_dq_check(ds)

    @task
    def build_dm(ds=None):
        return pipeline.do_build_dm(ds)

    @task
    def load_clickhouse(ds=None):
        if not pipeline.clickhouse_enabled():
            raise AirflowSkipException("ClickHouse выключен (CH_ENABLED != 1)")
        return pipeline.do_load_clickhouse(ds)

    load_stg() >> reconcile() >> load_ods() >> load_ref() >> build_dm() >> dq_check() >> load_clickhouse()
