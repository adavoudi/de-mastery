#!/usr/bin/env bash
# =============================================================================
# labs/smoke_core.sh  —  end-to-end smoke test for the `core` lab profile.
#
# Brings the profile up, waits for every service to be healthy, then exercises
# the full lakehouse path through the SAME surfaces a learner uses:
#   * Spark via the Spark Connect server (sc://spark:15002)
#   * Trino via the Iceberg catalog
#
# Steps:
#   1. Spark creates an Iceberg table (REST catalog, data in silo/S3)
#   2. Spark inserts rows
#   3. Trino reads the rows back  (second engine, same catalog)
#   4. Spark reads the rows back
#   5. Spark runs an Iceberg MERGE INTO (upsert)
#   6. Snapshot count is checked (>= 2 after the commits)
#   + final cross-engine check: Trino sees the merged state
#
# Exits non-zero on ANY failure.
#
# Usage:
#   bash labs/smoke_core.sh            # run smoke, leave stack up
#   bash labs/smoke_core.sh --down     # run smoke, then down -v
#   bash labs/smoke_core.sh --down-only # just tear down
# =============================================================================
set -euo pipefail

LABS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$LABS_DIR"

ENV_FILE=".env"
[ -f "$ENV_FILE" ] || { echo "No labs/.env found; copying from .env.example"; cp .env.example .env; }

DC=(docker compose --env-file "$ENV_FILE" --profile core)

DO_DOWN=0; DOWN_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --down|-d)   DO_DOWN=1 ;;
    --down-only) DOWN_ONLY=1; DO_DOWN=1 ;;
    --keep)      DO_DOWN=0 ;;
    *) echo "unknown arg: $arg"; exit 2 ;;
  esac
done

log()  { printf '\n\033[1;36m[smoke]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m  OK  %s\033[0m\n' "$*"; }
fail() { printf '\033[1;31m  FAIL %s\033[0m\n' "$*" >&2; }

teardown() { log "Tearing down (down -v)..."; "${DC[@]}" down -v --remove-orphans || true; }

if [ "$DOWN_ONLY" -eq 1 ]; then teardown; exit 0; fi

# Run a PySpark Connect client from the *jupyter* container, which ships the
# full pyspark client stack (grpcio, pandas, pyarrow). It connects to the Spark
# Connect server at sc://spark:15002 — exactly the path a learner's notebook
# uses. No local Java, no --packages.
spark_py() {
  "${DC[@]}" exec -T jupyter bash -lc '
    PY4J=$(ls $SPARK_HOME/python/lib/py4j-*-src.zip | head -1)
    export PYTHONPATH="$SPARK_HOME/python:$PY4J"
    export PYTHONWARNINGS=ignore
    python3 -'
}

trino_sql() {
  "${DC[@]}" exec -T trino trino --server http://localhost:8080 --catalog iceberg --execute "$1"
}

wait_healthy() {
  local svc="$1" tries="${2:-60}" cid st
  log "Waiting for '$svc' to be healthy (up to $tries x 5s)..."
  for ((i=1; i<=tries; i++)); do
    cid="$("${DC[@]}" ps -q "$svc" 2>/dev/null || true)"
    if [ -n "$cid" ]; then
      st="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || echo unknown)"
      if [ "$st" = "healthy" ]; then ok "$svc healthy"; return 0; fi
      # iceberg-rest has only a liveness check; probe its REST config endpoint.
      if [ "$svc" = "iceberg-rest" ] && [ "$st" = "running" ]; then
        if "${DC[@]}" exec -T trino curl -fsS http://iceberg-rest:8181/v1/config >/dev/null 2>&1; then
          ok "iceberg-rest responding"; return 0
        fi
      fi
    fi
    sleep 5
  done
  fail "$svc not healthy in time"; "${DC[@]}" logs --tail 50 "$svc" || true; return 1
}

spark_setup() {
  # Steps 1, 2, 4: create namespace+table, insert 3 rows, Spark reads them back.
  spark_py <<'PYEOF'
from pyspark.sql import SparkSession
TBL = "demo.smoke.orders"
s = SparkSession.builder.remote("sc://spark:15002").getOrCreate()
print("SPARK_VERSION", s.version)
s.sql("CREATE NAMESPACE IF NOT EXISTS demo.smoke")
s.sql(f"DROP TABLE IF EXISTS {TBL}")
s.sql(f"""CREATE TABLE {TBL} (
            order_id BIGINT, company STRING, amount DOUBLE, status STRING
          ) USING iceberg""")
print("STEP1_TABLE_CREATED")
s.sql(f"INSERT INTO {TBL} VALUES (1,'acme',100.0,'new'),(2,'acme',250.5,'new'),(3,'globex',75.0,'new')")
c = s.sql(f"SELECT count(*) c FROM {TBL}").collect()[0]["c"]
print("STEP2_INSERT_COUNT", c); assert c == 3, f"insert != 3: {c}"
print("STEP4_SPARK_READ", s.sql(f"SELECT count(*) c FROM {TBL}").collect()[0]["c"])
print("SPARK_SETUP_PASSED")
PYEOF
}

spark_merge() {
  # Steps 5, 6: Iceberg MERGE INTO (upsert) + snapshot-count check.
  spark_py <<'PYEOF'
from pyspark.sql import SparkSession
TBL = "demo.smoke.orders"
s = SparkSession.builder.remote("sc://spark:15002").getOrCreate()
s.sql(f"""MERGE INTO {TBL} t
          USING (SELECT * FROM VALUES (2,'acme',999.9,'paid'),(4,'initech',42.0,'new')
                 AS v(order_id,company,amount,status)) sdf
          ON t.order_id = sdf.order_id
          WHEN MATCHED THEN UPDATE SET t.amount=sdf.amount, t.status=sdf.status
          WHEN NOT MATCHED THEN INSERT *""")
rows = s.sql(f"SELECT count(*) c FROM {TBL}").collect()[0]["c"]
status2 = s.sql(f"SELECT status FROM {TBL} WHERE order_id=2").collect()[0]["status"]
print("STEP5_MERGE_ROWS", rows, "ORDER2_STATUS", status2)
assert rows == 4, f"expected 4 rows after merge, got {rows}"
assert status2 == "paid", f"order 2 status not updated: {status2}"
snaps = s.sql(f"SELECT count(*) c FROM {TBL}.snapshots").collect()[0]["c"]
print("STEP6_SNAPSHOTS", snaps); assert snaps >= 2, f"snapshots < 2: {snaps}"
print("SPARK_MERGE_PASSED")
PYEOF
}

main() {
  log "Bringing up the core profile (first run builds the Spark image + pulls)..."
  "${DC[@]}" up -d --build

  wait_healthy silo 40
  wait_healthy iceberg-rest 40
  wait_healthy trino 48
  wait_healthy spark 60
  wait_healthy jupyter 40 || fail "jupyter slow/unhealthy (non-fatal for smoke)"

  log "Steps 1,2,4  Spark via Spark Connect (sc://spark:15002): create + insert + read"
  SU_LOG="$(spark_setup)" || { fail "Spark setup crashed"; echo "$SU_LOG" | tail -30; return 1; }
  echo "$SU_LOG" | grep -E '^(SPARK_VERSION|STEP[124]|SPARK_SETUP_PASSED)' || true
  echo "$SU_LOG" | grep -q "STEP1_TABLE_CREATED"  || { fail "table not created"; return 1; }
  echo "$SU_LOG" | grep -q "STEP2_INSERT_COUNT 3" || { fail "insert != 3"; return 1; }
  echo "$SU_LOG" | grep -q "SPARK_SETUP_PASSED"   || { fail "setup did not complete"; return 1; }
  ok "Spark created table + inserted 3 rows"

  log "Step 3  Trino: read the rows back (second engine)"
  TR_COUNT="$(trino_sql "SELECT count(*) FROM smoke.orders" | tr -dc '0-9')"
  echo "    trino count -> $TR_COUNT"
  [ "$TR_COUNT" = "3" ] || { fail "Trino expected 3 rows, got '$TR_COUNT'"; return 1; }
  ok "Trino sees 3 rows (cross-engine read works)"

  log "Steps 5,6  Spark: Iceberg MERGE INTO + snapshot check"
  SM_LOG="$(spark_merge)" || { fail "Spark merge crashed"; echo "$SM_LOG" | tail -30; return 1; }
  echo "$SM_LOG" | grep -E '^(STEP[56]|ORDER2|SPARK_MERGE_PASSED)' || true
  echo "$SM_LOG" | grep -q "STEP5_MERGE_ROWS 4 ORDER2_STATUS paid" || { fail "MERGE result wrong"; return 1; }
  ok "Spark MERGE INTO applied (4 rows, order 2 => paid)"
  SNAP="$(echo "$SM_LOG" | sed -n 's/^STEP6_SNAPSHOTS \([0-9]*\).*/\1/p')"
  [ -n "$SNAP" ] && [ "$SNAP" -ge 2 ] || { fail "snapshots < 2 ('$SNAP')"; return 1; }
  ok "Iceberg snapshot history present ($SNAP snapshots)"

  log "Final cross-engine check: Trino sees the merged state"
  TR_FINAL="$(trino_sql "SELECT count(*) FROM smoke.orders" | tr -dc '0-9')"
  echo "    trino count -> $TR_FINAL"
  [ "$TR_FINAL" = "4" ] || { fail "Trino post-merge expected 4 rows, got '$TR_FINAL'"; return 1; }
  ok "Trino sees merged state (4 rows)"

  log "SMOKE PASSED  (Spark + Trino both read an Iceberg table backed by silo/S3)"
}

RC=0
main || RC=$?
if [ "$DO_DOWN" -eq 1 ]; then teardown; else log "Stack left running. Tear down: bash labs/smoke_core.sh --down-only"; fi
[ "$RC" -ne 0 ] && fail "SMOKE FAILED (exit $RC)"
exit "$RC"
