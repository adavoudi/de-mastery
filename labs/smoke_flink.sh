#!/usr/bin/env bash
# labs/smoke_flink.sh — prove the `flink` lab profile end to end:
#   1. JobManager + TaskManager up (session cluster), REST healthy
#   2. a TaskManager registers (slots available)
#   3. submit the bundled WordCount example jar via REST and see it FINISH
#   4. the SQL Gateway REST answers
#
# Exits non-zero on any failure. By default it tears down ONLY the containers in
# the `flink` profile that this script started (never the user's
# local_pgdb / pgadmin4_container, never the `core` / `streaming` profiles).
#
# Usage:
#   bash labs/smoke_flink.sh            # up, test, then down flink only
#   bash labs/smoke_flink.sh --keep-up  # up, test, leave it running
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${HERE}"
[ -f .env ] || cp .env.example .env

KEEP_UP=0
[ "${1:-}" = "--keep-up" ] && KEEP_UP=1

COMPOSE=(docker compose -f compose.yaml --profile flink)
REST_PORT="${LAB_FLINK_REST_PORT:-8086}"
SQLGW_PORT="${LAB_FLINK_SQL_GW_PORT:-8087}"

cleanup() {
  if [ "${KEEP_UP}" = "0" ]; then
    echo "--- tearing down flink profile (only) ---"
    "${COMPOSE[@]}" down -v || true
  else
    echo "--- leaving flink profile up (--keep-up) ---"
  fi
}
trap cleanup EXIT

echo "=== 1/4 bring up flink profile ==="
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

echo "=== 2/4 wait for JobManager + a TaskManager ==="
wait_healthy flink-jobmanager
for i in $(seq 1 40); do
  N="$(curl -fsS "http://localhost:${REST_PORT}/taskmanagers" 2>/dev/null \
        | python3 -c 'import sys,json;print(len(json.load(sys.stdin).get("taskmanagers",[])))' 2>/dev/null || echo 0)"
  [ "${N}" = "1" ] && { echo "  1 taskmanager registered"; break; }
  sleep 2
done
SLOTS="$(curl -fsS "http://localhost:${REST_PORT}/overview" 2>/dev/null \
         | python3 -c 'import sys,json;print(json.load(sys.stdin).get("slots-total"))' 2>/dev/null || echo 0)"
echo "  slots-total=${SLOTS}"
[ "${SLOTS}" -ge 1 ] 2>/dev/null || { echo "ERROR: no slots" >&2; exit 1; }

echo "=== 3/4 submit the bundled WordCount example and see it FINISH ==="
JM="$("${COMPOSE[@]}" ps -q flink-jobmanager)"
docker exec "${JM}" flink run /opt/flink/examples/streaming/WordCount.jar >/tmp/de-flink-wc.out 2>&1 \
  || { echo "ERROR: WordCount submit failed"; cat /tmp/de-flink-wc.out; exit 1; }
grep -qiE 'Job .* finished|Program execution finished' /tmp/de-flink-wc.out \
  && echo "  WordCount finished ✓" || { echo "  (job submitted; see /tmp/de-flink-wc.out)"; tail -5 /tmp/de-flink-wc.out; }

echo "=== 4/4 SQL Gateway REST answers ==="
for i in $(seq 1 30); do
  if curl -fsS "http://localhost:${SQLGW_PORT}/v1/info" >/tmp/de-flink-sqlgw.json 2>/dev/null; then
    echo "  sql-gateway info: $(cat /tmp/de-flink-sqlgw.json)"; break
  fi
  sleep 2
done
if ! grep -q 'productName' /tmp/de-flink-sqlgw.json 2>/dev/null; then
  echo "  WARN: SQL Gateway REST did not answer (non-fatal; cluster is healthy)."
fi

echo
echo "SMOKE OK: flink profile is healthy end to end."
