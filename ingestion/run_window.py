r"""Запустить загрузку за каждый день окна: одна команда на день.

Пример:
  python3 ingestion/run_window.py --start 2017-10-28 --end 2017-11-10 --cmd \
    "python3 ingestion/load_events.py data/olist/olist_orders_dataset.csv --table stg_orders --date-col order_purchase_timestamp --ds {ds}"

{ds} в команде заменяется на день. Дни идут по порядку от --start до --end включительно.
При первой ошибке останавливается (код 1); с --keep-going идёт дальше и
в конце печатает, какие дни упали. Это то, что на Неделе 19 станет задачей Airflow.
"""
import argparse
import datetime as dt
import shlex
import subprocess
import sys


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", required=True, help="первый день ГГГГ-ММ-ДД")
    ap.add_argument("--end", required=True, help="последний день ГГГГ-ММ-ДД")
    ap.add_argument("--cmd", required=True, help="команда с {ds}")
    ap.add_argument("--keep-going", action="store_true", help="не останавливаться на ошибке")
    a = ap.parse_args()
    if "{ds}" not in a.cmd:
        sys.exit("ОШИБКА: в --cmd нет {ds}")
    try:
        d0, d1 = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)
    except ValueError:
        sys.exit("ОШИБКА: даты ГГГГ-ММ-ДД")
    if d1 < d0:
        sys.exit("ОШИБКА: --end раньше --start")

    failed = []
    d = d0
    while d <= d1:
        cmd = a.cmd.replace("{ds}", d.isoformat())
        print(f"=== {d} ===", flush=True)
        rc = subprocess.run(shlex.split(cmd)).returncode
        if rc != 0:
            failed.append(d)
            if not a.keep_going:
                print(f"\nОСТАНОВЛЕНО на {d} (код {rc}). Исправьте и запустите с --start {d}.")
                sys.exit(1)
        d += dt.timedelta(days=1)

    n = (d1 - d0).days + 1
    print(f"\nГотово: дней {n}, успешно {n - len(failed)}, с ошибкой {len(failed)}")
    if failed:
        print("Упали дни:", ", ".join(map(str, failed)))
        sys.exit(1)


if __name__ == "__main__":
    main()
