"""FLK-10 — Debezium changelog -> upserted Iceberg table, reconciled vs DuckDB.

The infrastructure (Iceberg hadoop catalog on a per-test file:// warehouse, and
the debezium-json source tables) is created by the test. The learner's
`cdc_to_iceberg.py` supplies the upsert-capable target DDL, the upsert
INSERT…SELECT, the ALTER … ADD, and the phase-2 upsert.

Each `flink_sql.run(...)` executes in a fresh JVM, but the Iceberg hadoop
catalog persists on disk, so later runs re-`CREATE CATALOG` and see the table.
We reconcile the final table against an independent DuckDB fold of the same
change events (G6), assert a snapshot was committed (commit-on-checkpoint), and
assert schema evolution leaves old rows NULL for the new column.
"""

import glob
import json
from pathlib import Path

import duckdb

CAT = "CREATE CATALOG ice WITH ('type'='iceberg','catalog-type'='hadoop','warehouse'='{wh}')"

# ---- the change events -----------------------------------------------------
PHASE1 = [
    {"before": None, "after": {"id": 1, "name": "alice", "amt": 100}, "op": "c"},
    {"before": None, "after": {"id": 2, "name": "bob", "amt": 50}, "op": "c"},
    {"before": None, "after": {"id": 4, "name": "dan", "amt": 40}, "op": "c"},
    {"before": {"id": 1, "name": "alice", "amt": 100},
     "after": {"id": 1, "name": "alice", "amt": 175}, "op": "u"},
    {"before": {"id": 2, "name": "bob", "amt": 50}, "after": None, "op": "d"},
]
PHASE2 = [
    {"before": None,
     "after": {"id": 3, "name": "carol", "amt": 70, "region": "EU"}, "op": "c"},
    {"before": {"id": 1, "name": "alice", "amt": 175},
     "after": {"id": 1, "name": "alice", "amt": 120, "region": "US"}, "op": "u"},
]


def _write(path: Path, events):
    path.write_text("".join(json.dumps(e) + "\n" for e in events))


def _src_ddl(name, path, with_region=False):
    cols = "id INT, name STRING, amt INT" + (", region STRING" if with_region else "")
    return (f"CREATE TABLE {name} ({cols}, PRIMARY KEY (id) NOT ENFORCED) WITH "
            f"('connector'='filesystem','path'='{path}','format'='debezium-json')")


def _snapshot_count(wh: Path, table="customers") -> int:
    """Count committed Iceberg snapshots by reading the latest metadata.json."""
    meta_dir = wh / "db" / table / "metadata"
    metas = sorted(meta_dir.glob("v*.metadata.json"))
    if not metas:
        return 0
    # version-hint points at the current metadata version
    latest = metas[-1]
    data = json.loads(latest.read_text())
    return len(data.get("snapshots", []))


def _oracle_phase1():
    con = duckdb.connect()
    con.execute("CREATE TABLE t(id INT, name VARCHAR, amt INT)")
    for e in PHASE1:
        if e["op"] == "c":
            a = e["after"]
            con.execute("INSERT INTO t VALUES (?,?,?)", [a["id"], a["name"], a["amt"]])
        elif e["op"] == "u":
            a = e["after"]
            con.execute("UPDATE t SET name=?, amt=? WHERE id=?", [a["name"], a["amt"], a["id"]])
        elif e["op"] == "d":
            con.execute("DELETE FROM t WHERE id=?", [e["before"]["id"]])
    return sorted(con.execute("SELECT id,name,amt FROM t").fetchall())


def _oracle_phase2():
    con = duckdb.connect()
    con.execute("CREATE TABLE t(id INT, name VARCHAR, amt INT, region VARCHAR)")
    for e in PHASE1:
        if e["op"] == "c":
            a = e["after"]
            con.execute("INSERT INTO t VALUES (?,?,?,NULL)", [a["id"], a["name"], a["amt"]])
        elif e["op"] == "u":
            a = e["after"]
            con.execute("UPDATE t SET name=?, amt=? WHERE id=?", [a["name"], a["amt"], a["id"]])
        elif e["op"] == "d":
            con.execute("DELETE FROM t WHERE id=?", [e["before"]["id"]])
    for e in PHASE2:
        if e["op"] == "c":
            a = e["after"]
            con.execute("INSERT INTO t VALUES (?,?,?,?)",
                        [a["id"], a["name"], a["amt"], a.get("region")])
        elif e["op"] == "u":
            a = e["after"]
            con.execute("UPDATE t SET name=?, amt=?, region=? WHERE id=?",
                        [a["name"], a["amt"], a.get("region"), a["id"]])
        elif e["op"] == "d":
            con.execute("DELETE FROM t WHERE id=?", [e["before"]["id"]])
    return sorted(con.execute("SELECT id,name,amt,region FROM t").fetchall())


def _final_state_rows(rows, arity):
    """Collapse a changelog read-back into surviving rows (by full tuple)."""
    counts = {}
    for r in rows:
        key = tuple(r[1:1 + arity])
        counts[key] = counts.get(key, 0) + (1 if r[0] in ("+I", "+U") else -1)
    return sorted(k for k, c in counts.items() if c > 0)


def _run_phase1(flink_sql, submission, wh, src_path):
    cat = CAT.format(wh=f"file://{wh}")
    flink_sql.run(
        ddl=[cat, "CREATE DATABASE IF NOT EXISTS ice.db",
             submission.target_ddl("ice.db.customers"),
             _src_ddl("cdc1", src_path)],
        inserts=[submission.upsert_sql("cdc1", "ice.db.customers")],
        query="SELECT 1", mode="streaming", limit=1)


def test_phase1_reconciles(flink_sql, submission, tmp_path):
    wh = tmp_path / "wh"
    src = tmp_path / "p1.jsonl"
    _write(src, PHASE1)
    _run_phase1(flink_sql, submission, wh, src)
    rows = flink_sql.run(
        ddl=[CAT.format(wh=f"file://{wh}")],
        query="SELECT id, name, amt FROM ice.db.customers",
        mode="streaming", limit=100)
    got = _final_state_rows(rows, 3)
    want = [(str(i), n, str(a)) for i, n, a in _oracle_phase1()]
    assert got == want, f"got {got} want {want}"


def test_commit_on_checkpoint(flink_sql, submission, tmp_path):
    wh = tmp_path / "wh"
    src = tmp_path / "p1.jsonl"
    _write(src, PHASE1)
    _run_phase1(flink_sql, submission, wh, src)
    # the committed snapshot(s) prove the sink committed on checkpoint/complete
    assert _snapshot_count(wh) >= 1, "no Iceberg snapshot committed by the sink"


def test_schema_evolution_reconciles(flink_sql, submission, tmp_path):
    wh = tmp_path / "wh"
    src1 = tmp_path / "p1.jsonl"
    src2 = tmp_path / "p2.jsonl"
    _write(src1, PHASE1)
    _write(src2, PHASE2)
    _run_phase1(flink_sql, submission, wh, src1)
    # evolve + phase 2
    cat = CAT.format(wh=f"file://{wh}")
    flink_sql.run(
        ddl=[cat, submission.evolve_ddl("ice.db.customers"),
             _src_ddl("cdc2", src2, with_region=True)],
        inserts=[submission.upsert_sql_v2("cdc2", "ice.db.customers")],
        query="SELECT 1", mode="streaming", limit=1)
    rows = flink_sql.run(
        ddl=[cat],
        query="SELECT id, name, amt, region FROM ice.db.customers",
        mode="streaming", limit=100)
    got = _final_state_rows(rows, 4)
    want = [(str(i), n, str(a), (r if r is not None else None))
            for i, n, a, r in _oracle_phase2()]
    assert got == want, f"got {got} want {want}"
    # an old row (id survived from phase 1, never touched in phase 2) must read
    # NULL for the new column (region is field index 4: kind,id,name,amt,region)
    assert any(r[4] is None for r in got_rows_as_dicts(rows)), \
        "a pre-evolution row should read NULL for the new column"


def got_rows_as_dicts(rows):
    """Latest row per id as dicts, to check region NULLs on old rows."""
    latest = {}
    for r in rows:
        key = r[1]  # id
        if r[0] in ("+I", "+U"):
            latest[key] = r
        elif r[0] in ("-D",) and key in latest:
            latest.pop(key, None)
    return list(latest.values())
