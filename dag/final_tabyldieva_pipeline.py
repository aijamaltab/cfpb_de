"""final_tabyldieva_pipeline: загрузка за дату ds (stg -> ods) и запуск DAG витрин за ту же дату.

Источники (ФТ-1):
  API   CFPB Consumer Complaint Database  -> stg_cfpb_complaints   события за ds
  CSV   data/fred/DFF.csv (ставка ФРС)     -> stg_fred_dff          событие с датой, строка за ds
  CSV   data/ref/dim_company.csv           -> stg_dim_company       справочник целиком
  CSV   data/ref/company_bank_map.csv      -> stg_company_bank_map  справочник целиком
Дальше ods (типы, ключи), сверка с API, проверки качества, затем TriggerDagRunOperator запускает
final_tabyldieva_datamart с той же logical date. Каждая задача пишет строку в <логин>_stg.etl_load_log.
Все шаги идемпотентны (delete + insert по ds, справочники целиком): повторный запуск даты не меняет число строк.
Подключение к PostgreSQL: Airflow Connection aiacademy_pg_tabyldieva (пароля в коде нет).
catchup выключен: история 2023-2026 уже загружена DAG cfpb_final; здесь работа по расписанию и ручной запуск за дату.

Какой день грузится (задача resolve_ds):
  ручной запуск (Trigger DAG, в том числе с Logical date преподавателя)  ->  ровно эта дата;
  запуск по расписанию  ->  Logical date минус 22 дня. CFPB публикует жалобы с задержкой до 15 дней, за «вчера» API часто
  отдаёт 0, поэтому ночной запуск берёт уже устоявшийся день и продолжает окно проекта (с 1 октября) без дыр.
  В журнале etl_load_log и в витринах стоит загруженный день, а не Logical date запуска.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Рабочая копия лежит в ~/work/dags/ (так её видит Airflow стенда), исходник в final_project/dag/: ищем ingestion там и там.
_HERE = Path(__file__).resolve().parent
for _p in (_HERE.parent / "ingestion", _HERE / "final_project" / "ingestion"):
    if (_p / "cfpb_pipeline.py").is_file():
        sys.path.insert(0, str(_p))
        break

import cfpb_pipeline as cp      # noqa: E402
import common                  # noqa: E402
import csv_sources as cs        # noqa: E402

DAG_ID = "final_tabyldieva_pipeline"
DATAMART_DAG_ID = "final_tabyldieva_datamart"

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
    schedule="@daily",
    catchup=False,
    max_active_runs=1,                       # по одному дню: не нагружаем API
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["final_project", "cfpb", "tabyldieva"],
    doc_md=__doc__,
    params={"cfpb_base": Param(None, type=["null", "string"], description="Только для проверки НФТ-4: недоступный адрес API, например https://localhost:9/x. Пусто = настоящий адрес. Действует только на этот запуск.")},
) as dag:
    # в имени вида <логин>.final_tabyldieva_pipeline префикс добавляет стенд; витрины запускаем с тем же префиксом
    PREFIX = dag.dag_id[: -len(DAG_ID)] if dag.dag_id.endswith(DAG_ID) else ""
    DATAMART_FULL_ID = PREFIX + DATAMART_DAG_ID

    @task
    def resolve_ds(ds=None, dag_run=None):
        """Какой день грузит этот запуск: ручной -> ровно ds; по расписанию -> ds минус 22 дня (см. описание DAG)."""
        day = common.effective_ds(ds, dag_run.run_type if dag_run else None)
        print(f"запуск {dag_run.run_type if dag_run else '?'}, Logical date {ds}: грузим день {day}")
        return day

    @task
    def load_stg_complaints(day):
        return cp.do_load_stg(day)

    @task
    def reconcile(day):
        cp.do_reconcile(day)

    @task
    def load_ods_complaints(day):
        return cp.do_load_ods(day)

    @task
    def load_stg_fred(day):
        return cs.do_load_stg_fred(day)

    @task
    def load_ods_fred(day):
        return cs.do_load_ods_fred(day)

    @task
    def load_stg_refs(day):
        return cs.do_load_stg_refs(day)

    @task
    def load_ods_refs(day):
        return cs.do_load_ods_refs(day)

    @task
    def load_ods_derived(day):
        return cp.do_load_ref(day)            # ods_products, ods_companies: из ods_complaints

    @task
    def dq_ods(day):
        cp.do_dq_check(day)

    @task
    def check_datamart_ready():
        """Новые DAG создаются на паузе: запуск паузного DAG встал бы в очередь молча. Лучше упасть сразу и громко."""
        from airflow.models import DagModel
        model = DagModel.get_dagmodel(DATAMART_FULL_ID)
        if model is None:
            raise RuntimeError(f"DAG {DATAMART_FULL_ID} не найден: положите final_tabyldieva_datamart.py в ~/work/dags/ и подождите 1-2 минуты")
        if model.is_paused:
            raise RuntimeError(f"DAG {DATAMART_FULL_ID} на паузе: включите его переключателем в списке DAG")

    trigger_datamart = TriggerDagRunOperator(
        task_id="trigger_datamart",
        trigger_dag_id=DATAMART_FULL_ID,
        # день, который загрузил этот запуск (ручной: Logical date, по расписанию: Logical date минус 22 дня)
        logical_date="{{ ti.xcom_pull(task_ids='resolve_ds') }}T00:00:00+00:00",
        reset_dag_run=True,                   # повтор (Clear) той же даты перезапускает витрины, а не падает
        wait_for_completion=False,
        conf={"ds": "{{ ti.xcom_pull(task_ids='resolve_ds') }}"},
    )

    day = resolve_ds()
    complaints = load_stg_complaints(day) >> reconcile(day) >> load_ods_complaints(day)
    fred = load_stg_fred(day) >> load_ods_fred(day)
    refs = load_stg_refs(day) >> load_ods_refs(day)
    derived = load_ods_derived(day)
    complaints >> derived
    [derived, fred, refs] >> dq_ods(day) >> trigger_datamart
    check_datamart_ready() >> trigger_datamart
