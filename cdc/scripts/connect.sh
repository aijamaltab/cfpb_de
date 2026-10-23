#!/usr/bin/env bash
# connect.sh -- команды к Kafka Connect одной строкой.
# Использование: scripts/connect.sh КОМАНДА [аргумент]

# Адрес Connect берём из переменной CONNECT_URL; если её нет -- адрес по умолчанию.
URL="${CONNECT_URL:-http://kafka-connect:8083}"

# -sS: молчать, но ПОКАЗЫВАТЬ ошибки. -w: дописать в конце код ответа HTTP.
# Connect в ответе на create возвращает конфиг вместе с паролями: прячем значения password звёздочками.
mask() { sed -E 's/("[^"]*[Pp]assword" *: *")[^"]*(")/\1***\2/g'; }
call() { curl -sS -w '\nHTTP %{http_code}\n' "$@" | mask; }

# Первый аргумент -- команда, второй -- имя коннектора или файл.
case "$1" in
  ping)     call "$URL/" ;;
  create)   call -X POST -H 'Content-Type: application/json' -d @"$2" "$URL/connectors" ;;
  status)   call "$URL/connectors/$2/status" ;;
  topics)   call "$URL/connectors/$2/topics" ;;
  delete)   call -X DELETE "$URL/connectors/$2" ;;
  *)        echo "команды: ping | create файл | status имя | topics имя | delete имя"; exit 2 ;;
esac
