# Переменные для CDC. Запуск:  . cdc/scripts/env.sh   (точка и пробел в начале обязательны)
# В файле нет паролей: пароли спрашиваются при запуске (read -s) и живут только в этом терминале.
export CONNECT_URL=http://kafka-connect:8083     # адрес Kafka Connect (REST API)
export KAFKA_BOOTSTRAP=kafka:19092               # адрес брокера Kafka
export PG_HOST=postgres PG_PORT=5432 PG_DBNAME=aiacademy
export CDC_USER="${CDC_USER:-tabyldieva}"          # роль коннектора-источника; на нашем стенде это владелец таблицы (отдельная роль debezium: см. cdc/README.md)
export SINK_USER=tabyldieva                      # логин, под которым приёмник пишет в tabyldieva_ods
export TOPIC_PREFIX=tabyldieva_site              # префикс топиков: tabyldieva_site.tabyldieva.complaint_cases
export CDC_SLOT=dbz_tabyldieva CDC_PUBLICATION=dbz_tabyldieva
export CONNECTOR_SOURCE=complaint-cases-source-tabyldieva CONNECTOR_SINK=sink-tabyldieva
if [ -z "${CDC_PASSWORD:-}" ]; then read -r -s -p "Пароль роли $CDC_USER: " CDC_PASSWORD; echo; export CDC_PASSWORD; fi
if [ -z "${SINK_PASSWORD:-}" ]; then
    if [ "$CDC_USER" = "$SINK_USER" ]; then export SINK_PASSWORD="$CDC_PASSWORD"      # тот же логин -> тот же пароль, второй раз не спрашиваем
    else read -r -s -p "Пароль $SINK_USER (для приёмника): " SINK_PASSWORD; echo; export SINK_PASSWORD; fi
fi
