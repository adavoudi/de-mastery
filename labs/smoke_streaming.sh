#!/usr/bin/env bash
# labs/smoke_streaming.sh — prove the `streaming` lab profile end to end:
#   1. Kafka up (KRaft)         -> create a topic, produce + consume
#   2. Schema Registry up       -> register an Avro schema, read it back
#   3. CDC Postgres up          -> create a table + row
#   4. Kafka Connect up         -> start a Debezium Postgres connector
#   5. mutate Postgres          -> SEE a change event on the CDC topic
#
# Exits non-zero on any failure. By default it tears down ONLY the containers in
# the `streaming` profile that this script started (never the user's
# local_pgdb / pgadmin4_container, never the `core` profile).
#
# Usage:
#   bash labs/smoke_streaming.sh            # up, test, then down streaming only
#   bash labs/smoke_streaming.sh --keep-up  # up, test, leave it running
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${HERE}"
[ -f .env ] || cp .env.example .env

KEEP_UP=0
[ "${1:-}" = "--keep-up" ] && KEEP_UP=1

COMPOSE=(docker compose -f compose.yaml --profile streaming)
UI_PORT="${LAB_UI_PORT:-8085}"
SR_PORT="${LAB_SR_PORT:-8081}"
CONNECT_PORT="${LAB_CONNECT_PORT:-8083}"
KAFKA_PORT="${LAB_KAFKA_PORT:-19092}"

cleanup() {
  if [ "${KEEP_UP}" = "0" ]; then
    echo "--- tearing down streaming profile (only) ---"
    "${COMPOSE[@]}" down -v || true
  else
    echo "--- leaving streaming profile up (--keep-up) ---"
  fi
}
trap cleanup EXIT

echo "=== 1/6 bring up streaming profile ==="
"${COMPOSE[@]}" up -d

wait_healthy() {
  local svc="$1" tries="${2:-60}"
  for i in $(seq 1 "${tries}"); do
    local cid; cid="$("${COMPOSE[@]}" ps -q "${svc}" 2>/dev/null || true)"
    if [ -n "${cid}" ]; then
      local st; st="$(docker inspect -f '{{.State.Health.Status}}' "${cid}" 2>/dev/null || echo none)"
      [ "${st}" = "healthy" ] && { echo "  ${svc} healthy"; return 0; }
      [ "${st}" = "none" ] && { echo "  ${svc} running (no healthcheck)"; return 0; }
    fi
    sleep 2
  done
  echo "ERROR: ${svc} not healthy in time" >&2
  "${COMPOSE[@]}" logs --tail 40 "${svc}" >&2 || true
  return 1
}

echo "=== 2/6 wait for kafka / schema-registry / postgres / connect ==="
wait_healthy kafka
wait_healthy schema-registry
wait_healthy cdc-postgres
wait_healthy connect

echo "=== 3/6 topic + produce/consume ==="
KT=(docker exec de-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092)
"${KT[@]}" --create --topic smoke --partitions 1 --replication-factor 1 --if-not-exists
echo "hello-streaming" | docker exec -i de-kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic smoke
GOT="$(docker exec de-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic smoke --from-beginning --max-messages 1 --timeout-ms 15000)"
echo "  consumed: ${GOT}"
[ "${GOT}" = "hello-streaming" ] || { echo "ERROR: produce/consume mismatch" >&2; exit 1; }

echo "=== 4/6 register an Avro schema in Schema Registry ==="
SCHEMA='{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"amount\",\"type\":\"double\"}]}"}'
curl -fsS -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "${SCHEMA}" "http://localhost:${SR_PORT}/subjects/orders-value/versions"
echo
VERS="$(curl -fsS "http://localhost:${SR_PORT}/subjects/orders-value/versions")"
echo "  orders-value versions: ${VERS}"
[ "${VERS}" = "[1]" ] || echo "  (schema registered; versions=${VERS})"

echo "=== 5/6 seed Postgres + start Debezium connector ==="
docker exec de-cdc-postgres psql -U "${LAB_PG_USER:-cdc}" -d "${LAB_PG_DB:-cdc}" -v ON_ERROR_STOP=1 -c \
  "CREATE TABLE IF NOT EXISTS customers (id serial primary key, name text);
   ALTER TABLE customers REPLICA IDENTITY FULL;
   INSERT INTO customers(name) VALUES ('alice');"
cat > /tmp/de-debezium.json <<JSON
{
  "name": "smoke-pg",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "cdc-postgres",
    "database.port": "5432",
    "database.user": "${LAB_PG_USER:-cdc}",
    "database.password": "${LAB_PG_PASSWORD:-cdc}",
    "database.dbname": "${LAB_PG_DB:-cdc}",
    "topic.prefix": "cdc",
    "plugin.name": "pgoutput",
    "table.include.list": "public.customers",
    "slot.name": "de_smoke_slot",
    "publication.autocreate.mode": "filtered"
  }
}
JSON
curl -fsS -X POST -H "Content-Type: application/json" \
  --data @/tmp/de-debezium.json "http://localhost:${CONNECT_PORT}/connectors" >/dev/null
echo "  connector submitted; waiting for it to run..."
for i in $(seq 1 30); do
  STATE="$(curl -fsS "http://localhost:${CONNECT_PORT}/connectors/smoke-pg/status" 2>/dev/null | grep -o '"state":"RUNNING"' | head -1 || true)"
  [ -n "${STATE}" ] && { echo "  connector RUNNING"; break; }
  sleep 2
done

echo "=== 6/6 mutate Postgres and SEE a change event ==="
docker exec de-cdc-postgres psql -U "${LAB_PG_USER:-cdc}" -d "${LAB_PG_DB:-cdc}" -c \
  "INSERT INTO customers(name) VALUES ('bob');"
# CDC topic is cdc.public.customers; expect at least the snapshot 'alice' + 'bob'
EVENT="$(docker exec de-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic cdc.public.customers \
  --from-beginning --max-messages 1 --timeout-ms 30000 2>/dev/null || true)"
if echo "${EVENT}" | grep -q '"name"'; then
  echo "  change event observed on cdc.public.customers ✓"
else
  echo "ERROR: no change event seen on cdc.public.customers" >&2
  "${COMPOSE[@]}" logs --tail 30 connect >&2 || true
  exit 1
fi

echo
echo "SMOKE OK: streaming profile is healthy end to end."
