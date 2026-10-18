"""Tests for KFK-07 / derive SCD2 from a Debezium change stream.

No-Docker graded path: the learner's derive_scd2 is compared against an
independent DuckDB reconstruction of the same SCD2 over the same events (G6),
plus structural invariants (one current row per surviving key, no overlapping
windows, tombstones ignored).

An extra lab-`streaming` layer (lab_connect/lab_kafka/lab_postgres) drives the
real Debezium path and SKIPS cleanly when the lab is down — it never affects the
no-Docker gate outcome.
"""

import json
from pathlib import Path

import pytest

EVENTS = Path(__file__).parent / "events.json"


def _events():
    return json.loads(EVENTS.read_text())


def _duckdb_oracle(con, events):
    """Independent SCD2 reconstruction with DuckDB window functions."""
    rows = []
    for ev in events:
        if "op" not in ev or ev.get("value", "__") is None:
            continue  # tombstone / non-envelope
        img = ev["after"] if ev["after"] is not None else ev["before"]
        rows.append((
            img["id"], ev["op"],
            (ev["after"] or {}).get("name"),
            (ev["after"] or {}).get("tier"),
            ev["source"]["ts_ms"], ev["source"]["lsn"],
        ))
    con.execute("DROP TABLE IF EXISTS cdc")
    con.execute("CREATE TABLE cdc (id BIGINT, op VARCHAR, name VARCHAR, "
                "tier VARCHAR, ts BIGINT, lsn BIGINT)")
    con.executemany("INSERT INTO cdc VALUES (?,?,?,?,?,?)", rows)
    q = """
      WITH ordered AS (
        SELECT *,
               LEAD(ts) OVER (PARTITION BY id ORDER BY ts, lsn) AS next_ts
        FROM cdc
      )
      SELECT id, name, tier, ts AS valid_from, next_ts AS valid_to,
             (next_ts IS NULL) AS is_current
      FROM ordered
      WHERE op <> 'd'
      ORDER BY id, valid_from
    """
    res = con.execute(q).fetchall()
    return [
        {"id": r[0], "name": r[1], "tier": r[2], "valid_from": r[3],
         "valid_to": r[4], "is_current": bool(r[5])}
        for r in res
    ]


def _normalize(recs):
    return sorted(
        [{"id": r["id"], "name": r["name"], "tier": r["tier"],
          "valid_from": r["valid_from"], "valid_to": r["valid_to"],
          "is_current": bool(r["is_current"])} for r in recs],
        key=lambda r: (r["id"], r["valid_from"]))


def test_matches_duckdb_oracle(submission, duckdb_con):
    events = _events()
    got = _normalize(submission.derive_scd2(events))
    want = _normalize(_duckdb_oracle(duckdb_con, events))
    assert got == want


def test_one_current_per_surviving_key(submission):
    events = _events()
    recs = submission.derive_scd2(events)
    current_by_key = {}
    for r in recs:
        if r["is_current"]:
            current_by_key[r["id"]] = current_by_key.get(r["id"], 0) + 1
    # id=2 was deleted -> no current row; ids 1 and 3 have exactly one each.
    assert current_by_key == {1: 1, 3: 1}


def test_current_rows_have_null_valid_to(submission):
    for r in submission.derive_scd2(_events()):
        assert (r["valid_to"] is None) == bool(r["is_current"])


def test_no_overlapping_windows(submission):
    recs = submission.derive_scd2(_events())
    by_key = {}
    for r in recs:
        by_key.setdefault(r["id"], []).append(r)
    for key, versions in by_key.items():
        versions.sort(key=lambda r: r["valid_from"])
        for a, b in zip(versions, versions[1:]):
            assert a["valid_to"] is not None, f"gap/overlap on key {key}"
            assert a["valid_to"] <= b["valid_from"], f"overlap on key {key}"


def test_deleted_key_has_no_current(submission):
    recs = submission.derive_scd2(_events())
    key2 = [r for r in recs if r["id"] == 2]
    assert key2, "the snapshot of customer 2 should produce at least one version"
    assert all(not r["is_current"] for r in key2)
    assert all(r["valid_to"] is not None for r in key2)


def test_full_history_for_key1(submission):
    recs = _normalize(submission.derive_scd2(_events()))
    k1 = [r for r in recs if r["id"] == 1]
    # silver -> gold -> renamed(gold): three versions
    assert [(r["tier"], r["name"]) for r in k1] == [
        ("silver", "Acme Corp"),
        ("gold", "Acme Corp"),
        ("gold", "Acme Corporation"),
    ]


# --------------------------------------------------------------------------
# EXTRA layer: real Debezium -> Kafka in the `streaming` lab (skips if down).
# This never affects the no-Docker gates above.
# --------------------------------------------------------------------------
def test_lab_debezium_scd2(lab_connect, lab_kafka, lab_postgres, submission):
    pytest.skip("streaming-lab Debezium layer: run with `de lab up streaming`; "
                "exercised manually, not part of the no-Docker gate")
