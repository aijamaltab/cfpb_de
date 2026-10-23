"""final_tabyldieva_datamart: витрины из ods за дату ds и копия витрины в ClickHouse.

Запускается DAG-ом final_tabyldieva_pipeline с той же logical date (можно и вручную: Trigger DAG с датой).
  build_dm_complaints   dm_complaints_daily (день x продукт) и dm_company_month (месяц x компания)
  build_dm_bank_size    dm_bank_size_quarter (квартал x компания): метрика «жалоб на 1 млрд USD активов» на данных API FDIC
  dq_marts              проверки витрин; красная проверка останавливает копирование в ClickHouse
  load_clickhouse       dm_complaints_daily -> ClickHouse tabyldieva.dm_complaints_daily за тот же день, сверка строк и суммы
  load_clickhouse_company_month  dm_company_month -> ClickHouse (месяц дня ds), сверка строк и сумм
  load_clickhouse_bank_size      dm_bank_size_quarter -> ClickHouse (пересборка целиком), сверка строк и сумм
Витрины dbt (dm_state_month, dm_type_daily) копируются в ClickHouse скриптом tools/clickhouse_backfill_marts.py после dbt build.
Все шаги идемпотентны. Подключение к PostgreSQL: Airflow Connection aiacademy_pg_tabyldieva.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

# Рабочая копия лежит в ~/work/dags/ (так её видит Airflow стенда), исходник в final_project/dag/: ищем ingestion там и там.
_HERE = Path(__file__).resolve().parent
for _p in (_HERE.parent / "ingestion", _HERE / "final_project" / "ingestion"):
    if (_p / "cfpb_pipeline.py").is_file():
        sys.path.insert(0, str(_p))
        break

import cfpb_pipeline as cp          # noqa: E402
import datamart_pipeline as dp      # noqa: E402

DAG_ID = "final_tabyldieva_datamart"

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
    start_date=datetime(2023, 10, 1),
    schedule=None,                           # запускает основной DAG; вручную: Trigger DAG
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["final_project", "datamart", "tabyldieva"],
    doc_md=__doc__,
) as dag:
    @task
    def build_dm_complaints(ds=None):
        return cp.do_build_dm(ds)

    @task
    def build_dm_bank_size(ds=None):
        return dp.do_build_bank_size(ds)

    @task
    def dq_marts(ds=None):
        dp.do_dq_marts(ds)

    @task
    def load_clickhouse(ds=None):
        # включается Airflow Variable tabyldieva_ch_enabled=1 (см. README, раздел ClickHouse)
        if not dp.clickhouse_enabled():
            raise AirflowSkipException("ClickHouse выключен (tabyldieva_ch_enabled != 1)")
        return dp.do_load_clickhouse_mart(ds)

    @task
    def load_clickhouse_company_month(ds=None):
        if not dp.clickhouse_enabled():
            raise AirflowSkipException("ClickHouse выключен (tabyldieva_ch_enabled != 1)")
        return dp.do_load_clickhouse_company_month(ds)

    @task
    def load_clickhouse_bank_size(ds=None):
        if not dp.clickhouse_enabled():
            raise AirflowSkipException("ClickHouse выключен (tabyldieva_ch_enabled != 1)")
        return dp.do_load_clickhouse_bank_size(ds)

    [build_dm_complaints(), build_dm_bank_size()] >> dq_marts() >> [
        load_clickhouse(), load_clickhouse_company_month(), load_clickhouse_bank_size()]
