"""Phase 5 (streamhouse / STH) de_testkit fixture tests.

Fast, always-run tests cover the pure-Python helpers (the oracle, artifact
discovery, availability reasons). Heavy tests exercise the real native path
(Paimon / Iceberg embedded Flink SQL, pypaimon serving, Fluss local cluster) and
SKIP cleanly when the vendored artifacts are missing — so a bare machine stays
green while a provisioned machine (this one) verifies for real.

Every heavy test has an explicit per-test timeout; nothing iterates an unbounded
stream (batch reads, bounded scans, or a deadline that cancels the job).
"""

from pathlib import Path

import pytest

from de_testkit import streamhouse_support as sh

LABS = Path(__file__).parents[2] / "labs"


# ---------------------------------------------------------------------------
# layer-0: fetch script + artifact discovery (always run, fast)
# ---------------------------------------------------------------------------
def test_fetch_streamhouse_script_exists():
    assert (LABS / "fetch_streamhouse.sh").exists()
    assert (LABS / "smoke_streamhouse.sh").exists()


def test_availability_reasons_mention_fetch_command():
    # When something is missing the reason must name the exact fetch command.
    for fn in (sh.paimon_available, sh.iceberg_available, sh.fluss_available,
               sh.serving_available):
        ok, reason = fn()
        if not ok:
            assert "fetch" in reason.lower(), reason


# ---------------------------------------------------------------------------
# layer-1: the oracle (pure Python, always run)
# ---------------------------------------------------------------------------
def test_oracle_agrees_order_insensitive():
    streamed = [["+I", "1", "150"], ["+I", "2", "50"]]
    oracle = [[2, 50], [1, 150]]            # different order, ints
    assert sh.compare_streaming_vs_batch(streamed, oracle) is True


def test_oracle_drops_rowkind_by_default():
    streamed = [["+U", "1", "150"]]
    oracle = [[1, 150]]
    assert sh.compare_streaming_vs_batch(streamed, oracle) is True


def test_oracle_accepts_dict_rows():
    streamed = [{"id": 1, "bal": 150}, {"id": 2, "bal": 50}]
    oracle = [{"bal": 50, "id": 2}, {"bal": 150, "id": 1}]
    assert sh.compare_streaming_vs_batch(streamed, oracle, drop_rowkind=False)


def test_oracle_diff_is_clear_on_mismatch():
    with pytest.raises(AssertionError) as ei:
        sh.compare_streaming_vs_batch([["+I", "1", "150"]], [[1, 999]])
    msg = str(ei.value)
    assert "only in streaming" in msg and "only in batch" in msg


# ---------------------------------------------------------------------------
# layer-2: Paimon embedded Flink SQL (heavy; SKIPs if artifacts missing)
# ---------------------------------------------------------------------------
@pytest.mark.timeout(300)
def test_paimon_partial_update_batch(paimon_sql, sth_oracle):
    rows = paimon_sql.batch(
        ddl=["CREATE TABLE cust (id INT,name STRING,email STRING,phone STRING,"
             "PRIMARY KEY(id) NOT ENFORCED) WITH "
             "('merge-engine'='partial-update','bucket'='1')"],
        inserts=[
            "INSERT INTO cust VALUES (1,'Alice',CAST(NULL AS STRING),CAST(NULL AS STRING))",
            "INSERT INTO cust VALUES (1,CAST(NULL AS STRING),'a@x.com',CAST(NULL AS STRING))",
            "INSERT INTO cust VALUES (1,CAST(NULL AS STRING),CAST(NULL AS STRING),'555-1')",
        ],
        query="SELECT * FROM cust")
    # merged into one row; order-insensitive oracle vs the expected final state.
    sth_oracle(rows, [[1, "Alice", "a@x.com", "555-1"]])


@pytest.mark.timeout(300)
def test_paimon_changelog_dedup_state(paimon_sql, sth_oracle):
    rows = paimon_sql.batch(
        ddl=["CREATE TABLE acct (id INT,bal INT,PRIMARY KEY(id) NOT ENFORCED) "
             "WITH ('changelog-producer'='input','bucket'='1')"],
        inserts=["INSERT INTO acct VALUES (1,100)",
                 "INSERT INTO acct VALUES (1,150)",
                 "INSERT INTO acct VALUES (2,50)"],
        query="SELECT id,bal FROM acct")
    sth_oracle(rows, [[1, 150], [2, 50]])


@pytest.mark.timeout(300)
def test_paimon_streaming_bounded_has_deadline(paimon_sql):
    # A bounded streaming read of a tiny table must return promptly (the
    # watchdog cancels the job at the deadline; it never hangs).
    import time
    paimon_sql.batch(
        ddl=["CREATE TABLE t (id INT,v INT,PRIMARY KEY(id) NOT ENFORCED) "
             "WITH ('bucket'='1')"],
        inserts=["INSERT INTO t VALUES (1,10),(2,20)"])
    start = time.time()
    rows = paimon_sql.streaming(query="SELECT id,v FROM t", deadline_s=15,
                                timeout_s=120)
    elapsed = time.time() - start
    assert elapsed < 110, f"streaming read did not respect its deadline ({elapsed}s)"
    # at least the two committed rows are visible (final-state subset check).
    keys = {tuple(r[1:]) for r in rows}
    assert ("1", "10") in keys and ("2", "20") in keys


# ---------------------------------------------------------------------------
# layer-3: Iceberg embedded Flink SQL (heavy; SKIPs if artifacts missing)
# ---------------------------------------------------------------------------
@pytest.mark.timeout(300)
def test_iceberg_sink_commit_and_batch_read(iceberg_sql, tmp_path, sth_oracle):
    # a bounded debezium-json CDC source -> Iceberg sink commits on checkpoint.
    cdc = tmp_path / "cdc.jsonl"
    cdc.write_text(
        '{"before":null,"after":{"id":1,"amt":100},"op":"c"}\n'
        '{"before":{"id":1,"amt":100},"after":{"id":1,"amt":175},"op":"u"}\n')
    iceberg_sql.streaming(
        ddl=["CREATE TABLE ice.db.customers (id INT,amt INT,PRIMARY KEY(id) "
             "NOT ENFORCED) WITH ('format-version'='2')",
             f"CREATE TABLE cdc (id INT,amt INT,PRIMARY KEY(id) NOT ENFORCED) "
             f"WITH ('connector'='filesystem','path'='{cdc}',"
             "'format'='debezium-json')"],
        inserts=["INSERT INTO ice.db.customers SELECT id,amt FROM cdc"],
        query=None, deadline_s=60, timeout_s=180)
    rows = iceberg_sql.batch(query="SELECT count(*) c, sum(amt) s FROM ice.db.customers")
    sth_oracle(rows, [[1, 175]])


# ---------------------------------------------------------------------------
# layer-4: serving (pypaimon) + Fluss local cluster (heavy)
# ---------------------------------------------------------------------------
@pytest.mark.timeout(300)
def test_serving_paimon_reads_merged_state(paimon_sql, serving_query):
    paimon_sql.batch(
        ddl=["CREATE TABLE cust (id INT,name STRING,PRIMARY KEY(id) NOT "
             "ENFORCED) WITH ('bucket'='1')"],
        inserts=["INSERT INTO cust VALUES (1,'Alice'),(2,'Bob')"])
    rows = serving_query.paimon(str(paimon_sql.warehouse), "default.cust")
    by_id = {r["id"]: r["name"] for r in rows}
    assert by_id == {1: "Alice", 2: "Bob"}


@pytest.mark.timeout(180)
def test_fluss_cluster_starts_and_tears_down(fluss_cluster):
    import socket as _s
    c = fluss_cluster()
    # coordinator port is reachable once started.
    with _s.socket(_s.AF_INET, _s.SOCK_STREAM) as sk:
        sk.settimeout(2.0)
        assert sk.connect_ex(("127.0.0.1", c.coordinator_port)) == 0
    assert c.bootstrap.endswith(str(c.coordinator_port))
    # teardown is driven by the fixture finaliser; no assertion needed here.
