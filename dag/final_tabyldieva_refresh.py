"""final_tabyldieva_refresh: ежедневная перезагрузка последних дней, чтобы хранилище догоняло правки CFPB.

Зачем: CFPB меняет уже опубликованные дни (а поля «дата изменения» у API нет, CDC к чужой базе невозможен) — добавляет жалобы задним числом, снимает опубликованные и меняет итог ответа
компании (In progress -> Closed ...). Сверка по числу строк видит только первое и второе, поэтому здесь не сверяем,
а перезагружаем окно дней целиком: каждый день проходит ту же цепочку, что и обычная загрузка
(stg -> сверка с API -> ods -> проверки -> DAG витрин -> ClickHouse). Все шаги — delete + insert за день, повтор безопасен.

Как устроено: задача days считает список дней, затем для каждого дня запускается основной DAG final_tabyldieva_pipeline
с Logical date <день>T12:00:00+00:00 (ручной запуск грузит ровно эту дату). reset_dag_run=True: если запуск за этот день
уже был, он перезапускается, а не создаётся второй, поэтому в Grid один запуск на день. Основной DAG выполняет
по одному запуску за раз (max_active_runs=1), остальные ждут в очереди.

Какие дни:
  по расписанию (00:30 UTC, после ночного запуска) — дни перед «фронтиром»: фронтир = Logical date − 22 дня (его только что
  загрузил ночной запуск), окно = [фронтир − глубина, фронтир − 1]; глубина REFRESH_DAYS (28) каждую ночь
  и REFRESH_DAYS_WEEKLY (120) в ночь на четверг: статусы жалоб CFPB меняются до ~3 месяцев (замер в коде ниже);
  вручную — параметры start и end (включительно), например 2026-10-01 .. 2026-10-13; не больше MAX_DAYS дней.
Подключение к PostgreSQL берут задачи основного DAG (Airflow Connection aiacademy_pg_tabyldieva), здесь его нет.
"""
from datetime import date, datetime, timedelta

from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DAG_ID = "final_tabyldieva_refresh"
PIPELINE_DAG_ID = "final_tabyldieva_pipeline"
SCHEDULE_LAG_DAYS = 22      # как в основном DAG (common.SCHEDULE_LAG_DAYS)
REFRESH_DAYS = 28           # каждую ночь: 4 недели, где правок больше всего (поздние жалобы, первые смены статуса)
REFRESH_DAYS_WEEKLY = 120   # раз в неделю: хвост статусов. Замер 29.10: доля In progress по месяцам загрузки
                            # окт 36%, сен 30%, авг 10,5%, июл 0,6%, июн 0,1% -> статусы устаканиваются ~за 3 месяца
WEEKLY_DAY = 2              # date.weekday() Logical date: 2 = среда, запуск в ночь на четверг (вдали от показов в выходные)
MAX_DAYS = 130              # защита от случайного огромного окна при ручном запуске

try:
    from _lib.tenant import student_dag      # общий стенд курса: добавляет к dag_id префикс <логин>.
except ModuleNotFoundError as _e:            # вне стенда: обычный DAG
    if _e.name not in ("_lib", "_lib.tenant"):
        raise
    student_dag = None


def make_dag(**kwargs):
    if student_dag is not None:
        return student_dag(__file__, **kwargs)
    from airflow import DAG
    return DAG(**kwargs)


def refresh_window(logical_ds, run_type, start=None, end=None):
    """Список дней (ISO) для перезагрузки. Отдельная функция, чтобы её можно было проверить без Airflow."""
    if start or end:
        if not (start and end):
            raise ValueError("укажите оба параметра: start и end")
        d0, d1 = date.fromisoformat(start), date.fromisoformat(end)
    else:
        if getattr(run_type, "value", run_type) != "scheduled":
            raise ValueError("при ручном запуске укажите start и end (например 2026-10-01 и 2026-10-13)")
        logical = date.fromisoformat(logical_ds)
        frontier = logical - timedelta(days=SCHEDULE_LAG_DAYS)
        depth = REFRESH_DAYS_WEEKLY if logical.weekday() == WEEKLY_DAY else REFRESH_DAYS
        d0, d1 = frontier - timedelta(days=depth), frontier - timedelta(days=1)
    if d1 < d0:
        raise ValueError(f"end {d1} раньше start {d0}")
    if d1 >= date.today():
        raise ValueError(f"end {d1} не может быть сегодня или в будущем")
    n = (d1 - d0).days + 1
    if n > MAX_DAYS:
        raise ValueError(f"окно {n} дней больше MAX_DAYS={MAX_DAYS}")
    return [(d0 + timedelta(days=i)).isoformat() for i in range(n)]


with make_dag(
    dag_id=DAG_ID,
    start_date=datetime(2026, 10, 1),
    schedule="30 0 * * *",                   # 00:30 UTC: после ночного запуска основного DAG, до dbt-витрин в 02:00
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 0},
    tags=["final_project", "cfpb", "tabyldieva"],
    doc_md=__doc__,
    params={
        "start": Param(None, type=["null", "string"], description="Ручной запуск: первый день окна, YYYY-MM-DD. Пусто = окно по расписанию."),
        "end": Param(None, type=["null", "string"], description="Ручной запуск: последний день окна, YYYY-MM-DD (включительно)."),
    },
) as dag:
    PREFIX = dag.dag_id[: -len(DAG_ID)] if dag.dag_id.endswith(DAG_ID) else ""

    @task
    def days(ds=None, dag_run=None, params=None):
        params = params or {}
        out = refresh_window(ds, dag_run.run_type if dag_run else None, params.get("start"), params.get("end"))
        print(f"перезагружаю {len(out)} дн.: {out[0]} .. {out[-1]}")
        return [f"{d}T12:00:00+00:00" for d in out]

    TriggerDagRunOperator.partial(
        task_id="reload_day",
        trigger_dag_id=PREFIX + PIPELINE_DAG_ID,
        reset_dag_run=True,                  # повтор дня перезапускает существующий запуск, а не падает на «already exists»
        wait_for_completion=False,           # дни встают в очередь основного DAG (max_active_runs=1)
        conf={"note": "refresh"},
    ).expand(logical_date=days())
