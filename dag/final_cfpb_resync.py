"""DAG досинхронизации CFPB: находит дни, где загруженное разошлось с API (жалобы публикуются с опозданием), и перезагружает их.

Раз в неделю, затем кнопкой Trigger DAG при необходимости. Сначала контур банков, затем контур бюро (по очереди, API не нагружаем).
retries=0: долгий запуск не повторяем автоматически, смотрим журнал; повторный запуск безопасен.
"""
import sys
from datetime import datetime
from pathlib import Path

from airflow import __version__ as AF_VERSION
from airflow.decorators import task
from _lib.tenant import student_dag

sys.path.insert(0, str(Path(__file__).parent / "final_project" / "ingestion"))

import resync_pipeline as rp  # noqa: E402

_major, _minor = (int(x) for x in AF_VERSION.split(".")[:2])
SCHEDULE = {"schedule": "@weekly"} if (_major, _minor) >= (2, 4) else {"schedule_interval": "@weekly"}

with student_dag(
    __file__,
    dag_id="cfpb_resync",
    start_date=datetime(2026, 10, 4),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["final_project", "cfpb", "resync"],
    **SCHEDULE,
) as dag:

    @task
    def resync_bank():
        return rp.resync("bank")

    @task
    def resync_bureau():
        return rp.resync("bureau")

    resync_bank() >> resync_bureau()
