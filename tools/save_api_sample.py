#!/usr/bin/env python3
"""Сохраняет пример ответа API CFPB в docs/api_sample.json (его проверяет check_project.py ментора).

Берёт 2 жалобы за день через тот же curl, что и загрузчик. Текст жалоб убирается из примера: в нём бывают личные данные
(CFPB их маскирует, но в проект они не нужны).
Запуск из папки final_project:  python3 tools/save_api_sample.py 2026-09-14
"""
import json
import sys
from datetime import date
from pathlib import Path

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root / "ingestion"))

import cfpb_http        # noqa: E402
import load_cfpb        # noqa: E402

ds = date.fromisoformat(sys.argv[1] if len(sys.argv) > 1 else "2026-09-14")
r = cfpb_http.get(load_cfpb.BASE, load_cfpb.day_params(ds, 2, None), 30, 3)
if r.status_code != 200:
    sys.exit(f"HTTP {r.status_code}: {r.text[:200]}")
data = r.json()
hits = data.get("hits", {}).get("hits", [])
if not hits:
    sys.exit(f"за {ds} API ничего не вернул: возьмите другую дату")
for h in hits:
    src = h.get("_source", {})
    if src.get("complaint_what_happened"):
        src["complaint_what_happened"] = "(текст жалобы убран из примера)"
out = root / "docs" / "api_sample.json"
out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"сохранено {out}: жалоб в примере {len(hits)}, всего за день {data['hits']['total']['value']}")
