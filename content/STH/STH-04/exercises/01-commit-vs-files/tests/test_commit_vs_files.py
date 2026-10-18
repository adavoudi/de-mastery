"""STH-04 — stream a Debezium CDC feed into Iceberg and Paimon, reconciled to an
independent DuckDB fold (G6), and measure the commit-interval → file-count
trade-off via the deterministic Iceberg/Paimon *snapshot* count.

No Docker: the embedded Flink SQL runners (`iceberg_sql`, `paimon_sql`) write to
isolated tmp warehouses; a BOUNDED, TEMPORARY debezium-json source makes each
streaming sink finish and commit exactly one snapshot per job. Nothing iterates
an unbounded stream; every heavy test carries an explicit timeout.
"""

import glob
import json
from pathlib import Path

import duckdb
import pytest

# --- the change events (deterministic): one update, one delete, three creates --
EVENTS = [
    {"before": None, "after": {"id": 1, "name": "alice", "amt": 100}, "op": "c"},
    {"before": None, "after": {"id": 2, "name": "bob", "amt": 50}, "op": "c"},
    {"before": {"id": 1, "name": "alice", "amt": 100},
     "after": {"id": 1, "name": "alice", "amt": 175}, "op": "u"},
    {"before": {"id": 2, "name": "bob", "amt": 50}, "after": None, "op": "d"},
    {"before": None, "after": {"id": 3, "name": "carol", "amt": 70}, "op": "c"},
]
# three commit groups for the "many small commits" case (same total events)
GROUPS = [EVENTS[:2], EVENTS[2:4], EVENTS[4:]]


def _write(path: Path, events):
    path.write_text("".join(json.dumps(e) + "\n" for e in events))


def _oracle():
    """Independent current-state fold of the change events in DuckDB (G6)."""
    con = duckdb.connect()
    con.execute("CREATE TABLE t(id INT, name VARCHAR, amt INT)")
    for e in EVENTS:
        if e["op"] == "c":
            a = e["after"]
            con.execute("INSERT INTO t VALUES (?,?,?)", [a["id"], a["name"], a["amt"]])
        elif e["op"] == "u":
            a = e["after"]
            con.execute("UPDATE t SET name=?, amt=? WHERE id=?",
                        [a["name"], a["amt"], a["id"]])
        elif e["op"] == "d":
            con.execute("DELETE FROM t WHERE id=?", [e["before"]["id"]])
    return sorted(con.execute("SELECT id,name,amt FROM t").fetchall())


def _iceberg_snapshots(wh: Path, table="c") -> int:
    metas = sorted((wh / "db" / table / "metadata").glob("v*.metadata.json"))
    if not metas:
        return 0
    return len(json.loads(metas[-1].read_text()).get("snapshots", []))


def _paimon_snapshots(wh: Path, table="c") -> int:
    d = wh / "default.db" / table / "snapshot"
    return len(list(d.glob("snapshot-*"))) if d.exists() else 0


# ---------------------------------------------------------------------------
# Iceberg: single-commit job reconciles AND commits exactly one snapshot
# ---------------------------------------------------------------------------
@pytest.mark.timeout(240)
def test_iceberg_single_commit_reconciles(iceberg_sql, submission, tmp_path):
    src = tmp_path / "all.jsonl"
    _write(src, EVENTS)
    iceberg_sql.streaming(
        ddl=[submission.iceberg_target_ddl("ice.db.c"),
             submission.cdc_source_ddl("cdc", str(src))],
        inserts=[submission.apply_sql("cdc", "ice.db.c")],
        query=None, deadline_s=60, timeout_s=180)
    rows = iceberg_sql.batch(query="SELECT id, name, amt FROM ice.db.c")
    want = [[str(i), n, str(a)] for i, n, a in _oracle()]
    got = sorted([r[1:] for r in rows])
    assert got == sorted(want), f"got {got} want {sorted(want)}"
    # commit cadence: ONE job over the bounded source => exactly one snapshot.
    assert _iceberg_snapshots(iceberg_sql.warehouse) == 1, \
        "a single ingest job must commit exactly one Iceberg snapshot"


# ---------------------------------------------------------------------------
# Iceberg: three small jobs => three snapshots (more commits, more small files)
# ---------------------------------------------------------------------------
@pytest.mark.timeout(300)
def test_iceberg_many_commits_make_many_snapshots(iceberg_sql, submission, tmp_path):
    iceberg_sql.batch(ddl=[submission.iceberg_target_ddl("ice.db.c")])
    for i, group in enumerate(GROUPS):
        fp = tmp_path / f"g{i}.jsonl"
        _write(fp, group)
        iceberg_sql.streaming(
            ddl=[submission.cdc_source_ddl(f"cdc{i}", str(fp))],
            inserts=[submission.apply_sql(f"cdc{i}", "ice.db.c")],
            query=None, deadline_s=60, timeout_s=180)
    rows = iceberg_sql.batch(query="SELECT id, name, amt FROM ice.db.c")
    want = [[str(i), n, str(a)] for i, n, a in _oracle()]
    got = sorted([r[1:] for r in rows])
    assert got == sorted(want), f"got {got} want {sorted(want)}"
    # three ingest jobs => three snapshots: the small-files trade-off, measured.
    assert _iceberg_snapshots(iceberg_sql.warehouse) == len(GROUPS), \
        "each ingest job over the bounded source must add exactly one snapshot"


# ---------------------------------------------------------------------------
# Paimon: same CDC reconciles identically AND commits one snapshot per job
# ---------------------------------------------------------------------------
@pytest.mark.timeout(240)
def test_paimon_single_commit_reconciles(paimon_sql, submission, tmp_path):
    src = tmp_path / "all.jsonl"
    _write(src, EVENTS)
    paimon_sql.streaming(
        ddl=[submission.paimon_target_ddl("c"),
             submission.cdc_source_ddl("cdc", str(src))],
        inserts=[submission.apply_sql("cdc", "c")],
        query=None, deadline_s=60, timeout_s=180)
    rows = paimon_sql.batch(query="SELECT id, name, amt FROM c")
    want = [[str(i), n, str(a)] for i, n, a in _oracle()]
    got = sorted([r[1:] for r in rows])
    assert got == sorted(want), f"paimon got {got} want {sorted(want)}"
    assert _paimon_snapshots(paimon_sql.warehouse) == 1, \
        "a single Paimon ingest job must commit exactly one snapshot"


# ---------------------------------------------------------------------------
# Both engines agree with each other (streamhouse invariant via the oracle)
# ---------------------------------------------------------------------------
@pytest.mark.timeout(240)
def test_iceberg_matches_oracle_via_sth_oracle(iceberg_sql, submission, tmp_path,
                                               sth_oracle):
    src = tmp_path / "all.jsonl"
    _write(src, EVENTS)
    iceberg_sql.streaming(
        ddl=[submission.iceberg_target_ddl("ice.db.c"),
             submission.cdc_source_ddl("cdc", str(src))],
        inserts=[submission.apply_sql("cdc", "ice.db.c")],
        query=None, deadline_s=60, timeout_s=180)
    rows = iceberg_sql.batch(query="SELECT id, name, amt FROM ice.db.c")
    sth_oracle(rows, _oracle())
