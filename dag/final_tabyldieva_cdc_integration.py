"""final_tabyldieva_cdc_integration: контур CDC рядом с батчевым (интеграция CDC в общий проект).

Каждый час:
  check_cdc_health   в приёмнике tabyldieva_ods.ods_complaint_cases есть строки (иначе ошибка) и когда он менялся (предупреждение);
                     результат в <логин>_stg.etl_load_log (source = cdc_health)
  dbt_build_cdc      dbt build --select tag:cdc: слой CDC в dbt (int_cases_current, dm_open_cases, audit_cdc_vs_batch,
                     снимок статусов). Если dbt недоступен воркеру, задача пропускается: запускайте руками.
Включать ПОСЛЕ того, как коннекторы Debezium работают (cdc/README.md). Создаётся на паузе. Это дополнение, в минимум ФТТ не входит.
Подключение: Airflow Connection aiacademy_pg_tabyldieva (пароля в коде нет).
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

import cdc_integration as ci      # noqa: E402

DAG_ID = "final_tabyldieva_cdc_integration"

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
    schedule="@hourly",
    catchup=False,                           # догонять прошлое у потока смысла нет
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["final_project", "cdc", "tabyldieva"],
    doc_md=__doc__,
) as dag:
    @task
    def check_cdc_health(ds=None):
        return ci.do_check_cdc_health(ds)

    @task
    def dbt_build_cdc(ds=None):
        return ci.do_dbt_build_cdc(ds)

    check_cdc_health() >> dbt_build_cdc()
