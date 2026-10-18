#!/usr/bin/env bash
# labs/smoke_streamhouse.sh — prove the NATIVE streamhouse path end to end, with
# NO Docker, entirely under a wall-clock deadline. It drives the de_testkit
# streamhouse helpers directly (the same code the STH exercises grade against):
#
#   1. Paimon partial-update PK table: 3 partial writes merge into one row
#      (embedded Flink SQL, inBatchMode).
#   2. Serving: pypaimon (isolated venv) reads the merged Paimon table.
#   3. Iceberg-as-streaming-storage: a bounded debezium-json CDC source ->
#      Iceberg sink commits on checkpoint; batch read reconciles the final state.
#   4. (optional) Fluss local cluster lifecycle — only with --with-fluss, since
#      it starts 3 JVMs; always torn down.
#
# Everything runs under `timeout`; every background cluster is killed in a trap;
# nothing iterates an unbounded stream (batch reads / bounded scans with
# deadlines). SKIPs (exit 0 with a note) when the vendored artifacts are missing.
#
# Usage:
#   bash labs/smoke_streamhouse.sh              # Paimon + serving + Iceberg
#   bash labs/smoke_streamhouse.sh --with-fluss # also the Fluss local cluster
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"
cd "${ROOT}"

WITH_FLUSS=0
[ "${1:-}" = "--with-fluss" ] && WITH_FLUSS=1

cleanup() {
  # Kill any java we may have started (belt + braces; the fixtures also trap).
  pkill -9 -f "sth-fluss-" 2>/dev/null || true
}
trap cleanup EXIT

echo "=== streamhouse native smoke (no Docker) ==="

# A tiny Python driver that uses the de_testkit helpers exactly as the STH
# exercises do. It prints OK/SKIP lines; a hard error exits non-zero.
timeout 1200 uv run python - "${WITH_FLUSS}" <<'PY'
import sys, tempfile, time
from pathlib import Path
from de_testkit import streamhouse_support as sh

with_fluss = sys.argv[1] == "1"
rc = 0

# ---- 1. Paimon partial-update merge (embedded Flink SQL, batch) ----
ok, reason = sh.paimon_available()
if not ok:
    print(f"SKIP  paimon: {reason}")
else:
    wh = Path(tempfile.mkdtemp(prefix="sth-smoke-paimon-")) / "wh"
    runner = sh.build_paimon_runner(wh)
    rows = runner.batch(
        ddl=["CREATE TABLE cust (id INT,name STRING,email STRING,phone STRING,"
             "PRIMARY KEY(id) NOT ENFORCED) WITH "
             "('merge-engine'='partial-update','bucket'='1')"],
        inserts=[
            "INSERT INTO cust VALUES (1,'Alice',CAST(NULL AS STRING),CAST(NULL AS STRING))",
            "INSERT INTO cust VALUES (1,CAST(NULL AS STRING),'a@x.com',CAST(NULL AS STRING))",
            "INSERT INTO cust VALUES (1,CAST(NULL AS STRING),CAST(NULL AS STRING),'555-1')",
        ],
        query="SELECT * FROM cust")
    assert sh.compare_streaming_vs_batch(rows, [[1, "Alice", "a@x.com", "555-1"]])
    print(f"OK    1/4 Paimon partial-update merge -> {rows}")

    # ---- 2. Serving via pypaimon ----
    ok2, reason2 = sh.serving_available()
    if not ok2:
        print(f"SKIP  serving: {reason2}")
    else:
        srv = sh.ServingQuery()
        got = srv.paimon(str(wh), "default.cust")
        assert got and got[0]["id"] == 1 and got[0]["name"] == "Alice", got
        print(f"OK    2/4 pypaimon serving read -> {got}")

# ---- 3. Iceberg-as-streaming-storage (sink commits on checkpoint) ----
ok, reason = sh.iceberg_available()
if not ok:
    print(f"SKIP  iceberg: {reason}")
else:
    tmp = Path(tempfile.mkdtemp(prefix="sth-smoke-ice-"))
    cdc = tmp / "cdc.jsonl"
    cdc.write_text(
        '{"before":null,"after":{"id":1,"amt":100},"op":"c"}\n'
        '{"before":{"id":1,"amt":100},"after":{"id":1,"amt":175},"op":"u"}\n')
    ice = sh.build_iceberg_runner(tmp / "wh")
    ice.streaming(
        ddl=["CREATE TABLE ice.db.customers (id INT,amt INT,PRIMARY KEY(id) "
             "NOT ENFORCED) WITH ('format-version'='2')",
             f"CREATE TABLE cdc (id INT,amt INT,PRIMARY KEY(id) NOT ENFORCED) "
             f"WITH ('connector'='filesystem','path'='{cdc}',"
             "'format'='debezium-json')"],
        inserts=["INSERT INTO ice.db.customers SELECT id,amt FROM cdc"],
        query=None, deadline_s=60, timeout_s=170)
    rows = ice.batch(query="SELECT count(*) c, sum(amt) s FROM ice.db.customers")
    assert sh.compare_streaming_vs_batch(rows, [[1, 175]]), rows
    print(f"OK    3/4 Iceberg streaming sink + batch read -> {rows}")

# ---- 4. Fluss local cluster lifecycle (optional) ----
if not with_fluss:
    print("SKIP  4/4 Fluss local cluster (pass --with-fluss to run)")
else:
    ok, reason = sh.fluss_available()
    if not ok:
        print(f"SKIP  fluss: {reason}")
    else:
        import socket
        c = sh.start_fluss_cluster()
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2.0)
                assert s.connect_ex(("127.0.0.1", c.coordinator_port)) == 0
            print(f"OK    4/4 Fluss cluster up @ {c.bootstrap}")
        finally:
            c.shutdown()
            print("      Fluss cluster torn down")

print("SMOKE OK: streamhouse native path verified." if rc == 0 else "SMOKE FAILED")
sys.exit(rc)
PY

echo
echo "Confirming no leftover java processes we started..."
if pgrep -af "sth-fluss-" >/dev/null 2>&1; then
  echo "WARN: leftover Fluss processes found; killing." >&2
  pkill -9 -f "sth-fluss-" 2>/dev/null || true
fi
echo "SMOKE OK: streamhouse profile (native path) is healthy."
