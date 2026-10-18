"""STH-03/01 — Fluss PK table + lake tiering: streaming and batch reads match.

The lake tier a Fluss PK table writes into is a Paimon PK table. The graded
invariant is READ CONSISTENCY: once the data is in the lake tier, a bounded
STREAMING scan and a BATCH scan return the SAME set of rows (the latest value
per key). We reconcile both against an INDEPENDENT Python oracle (G6).

Part 1 runs on the embedded Paimon runner (no Docker) and is always reachable.
Part 2 boots a real native Fluss local cluster to prove the streaming-store half
of the tier exists; it SKIPs cleanly when the vendored Fluss server is absent.

Nothing iterates an unbounded stream: the streaming read is bounded with a
deadline; the Fluss cluster is torn down by its fixture finaliser.
"""

import socket

import pytest

# Upserts applied in order; same-id writes are non-adjacent so the final state
# is unambiguous and alt insert-batching cannot reorder a key's writes.
#   id 1: 100 then 175   -> 175
#   id 2: 50  then 90    -> 90
#   id 3: 70             -> 70
UPSERTS = [(1, 100), (2, 50), (1, 175), (3, 70), (2, 90)]


def _oracle():
    """Independent last-value-per-id fold (int rows)."""
    latest = {}
    for cid, bal in UPSERTS:          # left-to-right; last write wins
        latest[cid] = bal
    return [[cid, latest[cid]] for cid in sorted(latest)]


def _rows(paimon_sql, submission, mode, table="acct"):
    ddl = [submission.tier_table_ddl(table)]
    inserts = submission.upsert_inserts(table, UPSERTS)
    query = f"SELECT id, bal FROM {table}"
    if mode == "batch":
        return paimon_sql.batch(ddl=ddl, inserts=inserts, query=query, timeout_s=180)
    # bounded streaming scan with a deadline so it never hangs
    return paimon_sql.streaming(ddl=ddl, inserts=inserts, query=query,
                                deadline_s=15, timeout_s=120)


@pytest.mark.timeout(260)
def test_batch_read_matches_oracle(paimon_sql, submission, sth_oracle):
    rows = _rows(paimon_sql, submission, "batch")
    sth_oracle(rows, _oracle())


@pytest.mark.timeout(260)
def test_streaming_read_matches_oracle(paimon_sql, submission, sth_oracle):
    rows = _rows(paimon_sql, submission, "streaming")
    sth_oracle(rows, _oracle())


@pytest.mark.timeout(300)
def test_streaming_and_batch_reads_identical(paimon_sql, submission, sth_oracle):
    # THE read-consistency invariant: the two tiers agree with each other.
    # Use distinct table names so each read creates its own table on the shared
    # per-test warehouse (the runner reuses one warehouse across calls).
    batch_rows = _rows(paimon_sql, submission, "batch", table="acct_b")
    stream_rows = _rows(paimon_sql, submission, "streaming", table="acct_s")
    # compare streaming vs batch directly (both carry RowKind; drop it).
    def strip(rows):
        return [r[1:] for r in rows]
    sth_oracle(stream_rows, strip(batch_rows))


@pytest.mark.timeout(180)
def test_native_fluss_streaming_store_boots(fluss_cluster):
    # The streaming-store half of the tier: a real native Fluss local cluster
    # (ZK + coordinator + tablet server) comes up on free ports. SKIPs cleanly
    # when the vendored Fluss server is missing. Teardown is the fixture's job.
    c = fluss_cluster()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sk:
        sk.settimeout(3.0)
        assert sk.connect_ex(("127.0.0.1", c.coordinator_port)) == 0, (
            "Fluss coordinator should be reachable once the cluster is up")
    assert c.bootstrap.endswith(str(c.coordinator_port))
