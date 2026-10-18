"""Prove the Phase 2 de_testkit fixtures: spark_iceberg, delta_spark,
pyiceberg_catalog, and the streaming helper. Spark-backed tests skip cleanly if
the pinned JARs or Java are missing.
"""

from __future__ import annotations

import pyarrow as pa


# ---------------------------------------------------------------------------
# spark_iceberg: create / insert / MERGE INTO / snapshot query
# ---------------------------------------------------------------------------
def test_spark_iceberg_merge_and_snapshots(spark_iceberg):
    s = spark_iceberg
    s.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    s.sql("DROP TABLE IF EXISTS local.db.orders")
    s.sql("CREATE TABLE local.db.orders (id INT, amt DOUBLE) USING iceberg")
    s.sql("INSERT INTO local.db.orders VALUES (1, 10.0), (2, 20.0)")
    s.sql(
        "MERGE INTO local.db.orders t "
        "USING (SELECT 2 id, 25.0 amt UNION ALL SELECT 3, 30.0) u "
        "ON t.id = u.id "
        "WHEN MATCHED THEN UPDATE SET t.amt = u.amt "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    rows = sorted((r.id, r.amt) for r in s.sql("SELECT * FROM local.db.orders").collect())
    assert rows == [(1, 10.0), (2, 25.0), (3, 30.0)]
    snaps = s.sql("SELECT count(*) c FROM local.db.orders.snapshots").collect()[0].c
    assert snaps == 2  # one insert + one merge


# ---------------------------------------------------------------------------
# delta_spark: write + history
# ---------------------------------------------------------------------------
def test_delta_spark_write_and_history(delta_spark, tmp_path):
    s = delta_spark
    path = str(tmp_path / "delta_orders")
    s.range(0, 5).toDF("id").write.format("delta").save(path)
    assert s.read.format("delta").load(path).count() == 5
    from delta.tables import DeltaTable

    dt = DeltaTable.forPath(s, path)
    assert dt.history().count() >= 1
    # a second write adds a version
    s.range(5, 8).toDF("id").write.format("delta").mode("append").save(path)
    assert s.read.format("delta").load(path).count() == 8
    assert dt.history().count() >= 2


# ---------------------------------------------------------------------------
# pyiceberg_catalog: create / append / scan (no JVM)
# ---------------------------------------------------------------------------
def test_pyiceberg_create_append_scan(pyiceberg_catalog):
    cat = pyiceberg_catalog
    cat.create_namespace("db")
    data = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    tbl = cat.create_table("db.t", schema=data.schema)
    tbl.append(data)
    scanned = tbl.scan().to_arrow()
    assert scanned.num_rows == 3
    assert set(scanned.column_names) == {"id", "name"}
    # a second append grows the table and adds a snapshot
    tbl.append(pa.table({"id": [4], "name": ["d"]}))
    assert tbl.scan().to_arrow().num_rows == 4
    assert len(list(tbl.snapshots())) == 2


# ---------------------------------------------------------------------------
# streaming helper: file source + availableNow, final state == batch oracle
# ---------------------------------------------------------------------------
def test_streaming_file_source(spark, tmp_path, duckdb_con):
    from de_testkit import FileStreamSource, drive_available

    src = FileStreamSource(
        spark, tmp_path, fmt="json", schema="id INT, amt DOUBLE", name="events"
    )
    src.add([{"id": 1, "amt": 10.0}, {"id": 2, "amt": 20.0}])
    src.add([{"id": 1, "amt": 5.0}])
    reader = src.read_stream()
    agg = reader.groupBy("id").sum("amt")
    drive_available(agg, query_name="totals", output_mode="complete")
    rows = sorted(
        (r.id, r["sum(amt)"]) for r in spark.sql("SELECT * FROM totals").collect()
    )
    # DuckDB oracle over the same events
    duckdb_con.execute("CREATE TABLE e(id INT, amt DOUBLE)")
    duckdb_con.executemany(
        "INSERT INTO e VALUES (?, ?)", [(1, 10.0), (2, 20.0), (1, 5.0)]
    )
    oracle = sorted(
        (r[0], r[1])
        for r in duckdb_con.execute(
            "SELECT id, sum(amt) FROM e GROUP BY id"
        ).fetchall()
    )
    assert rows == oracle
