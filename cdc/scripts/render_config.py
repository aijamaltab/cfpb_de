#!/usr/bin/env python3
# render_config.py -- превращает шаблон коннектора в готовый json и проверяет его.
# Запуск: python3 scripts/render_config.py connectors/source.json.template > /tmp/source.json
import json
import os
import re
import sys

# Читаем шаблон: путь к нему передан первым аргументом.
text = open(sys.argv[1], encoding="utf-8").read()

# Ищем все ${ИМЯ} (только заглавные буквы: ${source.table} принадлежит коннектору, не нам).
names = set(re.findall(r"\$\{([A-Z][A-Z0-9_]*)\}", text))

# Какие из них не заданы в окружении.
missing = sorted(n for n in names if not os.environ.get(n))
if missing:
    sys.exit("ОШИБКА: не заданы переменные: " + ", ".join(missing))

# Подставляем значения вместо ${ИМЯ}.
text = re.sub(r"\$\{([A-Z][A-Z0-9_]*)\}", lambda m: os.environ[m.group(1)], text)

# Проверяем, что получился корректный json (лишняя запятая или одинарные кавычки его ломают).
try:
    cfg = json.loads(text)
except json.JSONDecodeError as e:
    sys.exit(f"ОШИБКА: не json: {e}")

# Три правила, нарушение которых ломает цепочку.
c = cfg["config"]
errors = []
if c.get("value.converter.schemas.enable") != "true":
    errors.append("value.converter.schemas.enable должен быть \"true\"")
if "connection.user" in c:
    errors.append("в приёмнике ключ называется connection.username, а не connection.user")
if c.get("insert.mode") == "insert":
    errors.append("insert.mode=insert даст UniqueViolation при перезапуске, нужен upsert")

# Есть ошибки -- печатаем и выходим с кодом 1; иначе выводим готовый json.
if errors:
    sys.exit("\n".join("ОШИБКА: " + e for e in errors))
print(json.dumps(cfg, ensure_ascii=False, indent=2))
