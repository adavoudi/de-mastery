"""Tests for LKH-06 / 01-scd2-delta.

Scenario: initial dim {1:'a', 2:'x'}; a batch of updates {1->'b' (changed),
2->'x' (unchanged), 3->'z' (new)} effective 2026-02-01. The learner's SCD2 MERGE
must close key 1's old version, insert 1:'b' and 3:'z', and leave 2 alone.

Oracle (G6): DuckDB computes the expected CURRENT dimension independently from the
same initial + update rows. Uses the `delta_spark` fixture (shared session with the
Delta extensions); SKIPS cleanly if the Delta JARs are absent.
"""

TABLE = "delta_dim"
INITIAL = [(1, "a", "2026-01-01"), (2, "x", "2026-01-01")]
UPDATES = [(1, "b", "2026-02-01"), (2, "x", "2026-02-01"), (3, "z", "2026-02-01")]
UPDATES_VIEW = "scd_updates"


def _setup(spark, tmp_path):
    loc = str(tmp_path / "delta_dim")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(
        f"""CREATE TABLE {TABLE}
            (bkey INT, attr STRING, valid_from STRING, valid_to STRING)
            USING delta LOCATION '{loc}'
            TBLPROPERTIES (delta.enableChangeDataFeed = true)"""
    )
    init_vals = ",".join(f"({k},'{a}','{d}',NULL)" for k, a, d in INITIAL)
    spark.sql(f"INSERT INTO {TABLE} VALUES {init_vals}")
    upd_vals = ",".join(f"({k},'{a}','{d}')" for k, a, d in UPDATES)
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW {UPDATES_VIEW} AS "
        f"SELECT * FROM VALUES {upd_vals} AS t(bkey, attr, eff)"
    )
    return loc


def _oracle_current(duckdb_con):
    """Expected current dimension: for each key, the attr after applying updates."""
    duckdb_con.execute("CREATE TABLE ini(bkey INT, attr VARCHAR)")
    duckdb_con.executemany("INSERT INTO ini VALUES (?,?)", [(k, a) for k, a, _ in INITIAL])
    duckdb_con.execute("CREATE TABLE upd(bkey INT, attr VARCHAR)")
    duckdb_con.executemany("INSERT INTO upd VALUES (?,?)", [(k, a) for k, a, _ in UPDATES])
    # current = updates take precedence; keys only in initial keep their attr.
    rows = duckdb_con.execute(
        """
        SELECT bkey, attr FROM upd
        UNION
        SELECT i.bkey, i.attr FROM ini i
        WHERE i.bkey NOT IN (SELECT bkey FROM upd)
        ORDER BY bkey
        """
    ).fetchall()
    return sorted((int(b), a) for b, a in rows)


def _current(spark):
    return sorted(
        (r["bkey"], r["attr"])
        for r in spark.sql(
            f"SELECT bkey, attr FROM {TABLE} WHERE valid_to IS NULL"
        ).collect()
    )


def test_current_matches_oracle(submission, delta_spark, duckdb_con, tmp_path):
    spark = delta_spark
    _setup(spark, tmp_path)
    submission.scd2_merge(spark, TABLE, UPDATES_VIEW)
    assert _current(spark) == _oracle_current(duckdb_con)


def test_one_open_row_per_key(submission, delta_spark, tmp_path):
    spark = delta_spark
    _setup(spark, tmp_path)
    submission.scd2_merge(spark, TABLE, UPDATES_VIEW)
    rows = spark.sql(
        f"SELECT bkey, COUNT(*) c FROM {TABLE} WHERE valid_to IS NULL GROUP BY bkey"
    ).collect()
    for r in rows:
        assert r["c"] == 1, f"key {r['bkey']} has {r['c']} open rows (expected 1)"
    open_keys = {r["bkey"] for r in rows}
    assert open_keys == {1, 2, 3}, f"wrong set of open keys: {open_keys}"


def test_history_preserved_for_changed_key(submission, delta_spark, tmp_path):
    spark = delta_spark
    _setup(spark, tmp_path)
    submission.scd2_merge(spark, TABLE, UPDATES_VIEW)
    # Key 1 changed a->b: the old 'a' row must still exist, now CLOSED.
    closed = spark.sql(
        f"SELECT bkey, attr, valid_to FROM {TABLE} "
        f"WHERE bkey = 1 AND attr = 'a' AND valid_to IS NOT NULL"
    ).collect()
    assert len(closed) == 1, "key 1's old version was not preserved+closed (SCD1 bug?)"
    assert closed[0]["valid_to"] == "2026-02-01"


def test_delta_log_grew(submission, delta_spark, tmp_path):
    spark = delta_spark
    loc = _setup(spark, tmp_path)
    before = spark.sql(f"DESCRIBE HISTORY {TABLE}").count()
    submission.scd2_merge(spark, TABLE, UPDATES_VIEW)
    after = spark.sql(f"DESCRIBE HISTORY {TABLE}").count()
    assert after > before, "no new Delta commits were made"
    import os
    commits = [
        f for f in os.listdir(os.path.join(loc, "_delta_log")) if f.endswith(".json")
    ]
    assert len(commits) >= 3, f"expected multiple _delta_log commits, found {commits}"


def test_change_data_feed(submission, delta_spark, tmp_path):
    spark = delta_spark
    _setup(spark, tmp_path)
    submission.scd2_merge(spark, TABLE, UPDATES_VIEW)
    cdf = spark.read.format("delta").option("readChangeFeed", "true").option(
        "startingVersion", 0
    ).table(TABLE)
    types = {r["_change_type"] for r in cdf.collect()}
    # Closing key 1 is an UPDATE (pre+post image); inserting new versions is INSERT.
    assert "update_preimage" in types and "update_postimage" in types, (
        f"expected update pre/postimage in CDF, got {types}"
    )
    assert "insert" in types, f"expected inserts in CDF, got {types}"
