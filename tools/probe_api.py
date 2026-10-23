#!/usr/bin/env python3
"""Разведка источника (аналог «скрипта проверки источника» у ментора). Ничего не пишет в базу.

  python3 tools/probe_api.py --ds 2026-09-01
  python3 tools/probe_api.py --ds 2026-09-05      # выходной день (суббота)
  python3 tools/probe_api.py --ds 2027-01-01      # будущая дата: ждём 0 строк или 4xx
  python3 tools/probe_api.py --ds 2026-09-01 --scan-from 2026-09-01 --scan-to 2026-09-30   # строк по дням, выбор окна

Показывает: статус, тип, размер, hits.total.value, структуру JSON, черновик колонок stg, сверку дат,
проверяет, что смещение frm даёт другие complaint_id. Образец ответа сохраняет в docs/samples/.
"""
import argparse, datetime as dt, json, pathlib, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / 'ingestion'))
import cfpb_http

BASE = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
EXPECTED = ["complaint_id", "date_received", "date_sent_to_company", "product", "sub_product", "issue",
            "sub_issue", "company", "state", "zip_code", "company_response", "company_public_response",
            "timely", "submitted_via", "tags"]


def call(ds, frm=0, size=3, extra=None):
    p = {"size": size, "frm": frm, "no_aggs": "true", "sort": "created_date_desc",
         "date_received_min": ds, "date_received_max": ds}
    p.update(extra or {})
    return cfpb_http.get(BASE, p, timeout=30, retries=2)


def flat(prefix, obj, out):
    if isinstance(obj, dict):
        for k, v in obj.items():
            flat(f"{prefix}_{k}" if prefix else k, v, out)
    else:
        out[prefix.lower()] = type(obj).__name__


def total(r):
    return r.json()["hits"]["total"]["value"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ds", required=True)
    ap.add_argument("--scan-from"); ap.add_argument("--scan-to")
    a = ap.parse_args()

    r = call(a.ds)
    print(f"HTTP {r.status_code}, content-type={r.headers.get('content-type')}, размер={len(r.content)} байт")
    if r.status_code != 200:
        print("Ответ:", r.text[:300]); sys.exit(1)
    j = r.json()
    pathlib.Path("docs/samples").mkdir(parents=True, exist_ok=True)
    pathlib.Path(f"docs/samples/cfpb_{a.ds}.json").write_text(json.dumps(j, ensure_ascii=False, indent=2)[:200000])
    print("Верхний уровень JSON:", list(j.keys()))
    hits = j["hits"]["hits"]
    print(f"hits.total.value = {total(r)}  (за день {a.ds}); строк в ответе: {len(hits)}")
    if not hits:
        print("ПУСТО. Для выходного/будущего дня это может быть нормой, для будущих проверок это тревога."); return
    cols = {}; flat("", hits[0]["_source"], cols)
    print("\nЧерновик колонок stg (всё text):")
    for c, t in cols.items(): print(f"  {c:28s} text   -- в JSON: {t}")
    print("\nНовые поля относительно контракта:", sorted(set(cols) - set(EXPECTED)) or "нет")
    print("Пропавшие поля:", sorted(set(EXPECTED) - set(cols)) or "нет")
    got = [h["_source"].get("date_received") for h in hits]
    print("date_received в ответе:", got, "<- должны начинаться с", a.ds)

    r2 = call(a.ds, frm=3)
    if r2.status_code == 200:
        ids1 = [h["_source"]["complaint_id"] for h in hits]
        ids2 = [h["_source"]["complaint_id"] for h in r2.json()["hits"]["hits"]]
        print("\nПагинация frm: страница 1", ids1, "страница 2", ids2,
              "-> OK, разные" if ids2 and not set(ids1) & set(ids2) else "-> ПРОБЛЕМА: пересекаются или пусто")
    r3 = call(a.ds, frm=9990, size=20)
    print(f"Глубокая страница frm=9990: HTTP {r3.status_code}", (r3.text[:150] if r3.status_code != 200 else f"строк {len(r3.json()['hits']['hits'])}"))
    r4 = call(a.ds, size=1000)
    print(f"size=1000: HTTP {r4.status_code}", (f"строк {len(r4.json()['hits']['hits'])}" if r4.status_code == 200 else r4.text[:150]))
    r5 = call(a.ds, extra={"nonexistent_param": "x"})
    print(f"Неверный параметр: HTTP {r5.status_code} (ждём игнор или 4xx; игнор = тихая опасность)")

    if a.scan_from and a.scan_to:
        d, end = dt.date.fromisoformat(a.scan_from), dt.date.fromisoformat(a.scan_to)
        print("\nСтрок по дням:"); rows = []
        while d <= end:
            n = total(call(d.isoformat(), size=1)); rows.append((d, n)); print(f"  {d} {d.strftime('%a')} {n}"); d += dt.timedelta(days=1)
        best = max(range(len(rows) - 13), key=lambda i: sum(n for _, n in rows[i:i + 14])) if len(rows) >= 14 else 0
        if len(rows) >= 14:
            w = rows[best:best + 14]
            print(f"\nЛучшее 14-дневное окно: {w[0][0]} .. {w[-1][0]}, строк {sum(n for _, n in w)}, пустых дней {sum(1 for _, n in w if n == 0)}")


if __name__ == "__main__":
    main()
