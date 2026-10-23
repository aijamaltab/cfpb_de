#!/usr/bin/env python3
# verify_cdc.py -- сверка: доехало ли всё из источника в приёмник (число строк и контрольная сумма всех значений).
# Запуск: PGHOST=postgres PGUSER=tabyldieva PGDATABASE=aiacademy python3 -I cdc/scripts/verify_cdc.py   (PGPASSWORD уже в окружении)
import sys

import psycopg2  # параметры подключения берёт из переменных PG*

PAIRS = [("tabyldieva.complaint_cases", "tabyldieva_ods.ods_complaint_cases", "complaint_id")]

cur = psycopg2.connect("").cursor()
bad = 0


def stats(table, key, cols):
    row = "md5(row(" + ", ".join(cols) + ")::text)"
    cur.execute(f"select count(*), coalesce(md5(string_agg({row}, '' order by {key})), '') from {table}")
    return cur.fetchone()


for src, dst, key in PAIRS:
    # Сравниваем все столбцы, кроме updated_at (у приёмника время может быть с другой точностью).
    cur.execute("select column_name from information_schema.columns "
                "where table_schema || '.' || table_name = %s and column_name <> 'updated_at' "
                "order by ordinal_position", (src,))
    cols = [r[0] for r in cur.fetchall()]
    a, b = stats(src, key, cols), stats(dst, key, cols)
    ok = a == b and a[0] > 0               # пустой источник не считается совпадением
    note = "" if a[0] else "   <- в источнике ноль строк, сверять нечего"
    print(("OK  " if ok else "FAIL"), src, a[0], "строк |", dst, b[0], "строк" + note)
    bad += 0 if ok else 1

sys.exit(1 if bad else 0)
