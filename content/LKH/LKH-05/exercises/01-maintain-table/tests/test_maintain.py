"""Tests for LKH-05 / 01-maintain-table.

The test seeds `local.lkh05.events` with many tiny single-batch files, captures a
DuckDB oracle of the original rows (G6), runs the learner's maintain(), then
asserts the layout improved while the data is byte-for-byte unchanged.

Uses the `spark_iceberg` fixture (shared session, `local` Hadoop catalog). SKIPS
cleanly if the pinned Iceberg JARs are absent.
"""

import pyarrow as pa

TABLE = "local.lkh05.events"
SORT_COL = "event_date"
N_BATCHES = 24          # tiny single-batch commits -> many small files
ROWS_PER_BATCH = 5


def _seed(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lkh05")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(
        f"CREATE TABLE {TABLE} (id INT, {SORT_COL} STRING, amt DOUBLE) USING iceberg"
    )
    for b in range(N_BATCHES):
        # event_date deliberately NOT in id order, so sorting reorders rows.
        day = (N_BATCHES - b) % 28 + 1
        vals = ",".join(
            f"({b * ROWS_PER_BATCH + i}, '2026-01-{day:02d}', {float(b + i)})"
            for i in range(ROWS_PER_BATCH)
        )
        spark.sql(f"INSERT INTO {TABLE} VALUES {vals}")


def _rows(spark):
    return sorted(
        (r["id"], r[SORT_COL], r["amt"])
        for r in spark.sql(f"SELECT id, {SORT_COL}, amt FROM {TABLE}").collect()
    )


def _file_count(spark):
    return spark.sql(f"SELECT COUNT(*) FROM {TABLE}.files").collect()[0][0]


def _avg_file_size(spark):
    return spark.sql(
        f"SELECT AVG(file_size_in_bytes) FROM {TABLE}.files"
    ).collect()[0][0]


def _snapshot_count(spark):
    return spark.sql(f"SELECT COUNT(*) FROM {TABLE}.snapshots").collect()[0][0]


def _run(submission, spark):
    """Seed, capture oracle + baseline, run the learner's maintain(). Returns
    (oracle_rows, files_before, avg_before)."""
    _seed(spark)
    oracle = _rows(spark)
    files_before = _file_count(spark)
    avg_before = _avg_file_size(spark)
    submission.maintain(spark, TABLE, SORT_COL)
    return oracle, files_before, avg_before


def test_data_unchanged(submission, spark_iceberg, duckdb_con):
    spark = spark_iceberg
    oracle, _, _ = _run(submission, spark)
    # DuckDB oracle: the captured original rows must equal the post-maintenance rows.
    expected = pa.table(
        {
            "id": [r[0] for r in oracle],
            SORT_COL: [r[1] for r in oracle],
            "amt": [r[2] for r in oracle],
        }
    )
    duckdb_con.register("expected", expected)
    oracle_set = duckdb_con.execute(
        f"SELECT id, {SORT_COL}, amt FROM expected ORDER BY id"
    ).fetchall()
    after = sorted(_rows(spark), key=lambda t: t[0])
    assert after == [tuple(r) for r in oracle_set], "maintenance changed the data"


def test_files_compacted(submission, spark_iceberg):
    spark = spark_iceberg
    _, files_before, _ = _run(submission, spark)
    files_after = _file_count(spark)
    # The seed makes dozens of files; compaction must collapse them to a handful.
    assert files_before >= N_BATCHES, "seed did not create many files"
    assert files_after <= 6, f"expected few files after compaction, got {files_after}"


def test_avg_file_size_grew(submission, spark_iceberg):
    spark = spark_iceberg
    _, _, avg_before = _run(submission, spark)
    avg_after = _avg_file_size(spark)
    # Files were genuinely combined, not just re-snapshotted.
    assert avg_after > avg_before, "average file size did not increase"
    assert avg_after > 1500, f"files still too small on average: {avg_after}"


def test_snapshots_expired(submission, spark_iceberg):
    spark = spark_iceberg
    _run(submission, spark)
    snaps = _snapshot_count(spark)
    # Old per-batch snapshots must be reclaimed; keep only the compacted snapshot.
    assert snaps <= 2, f"old snapshots were not expired (snapshots={snaps})"
    assert snaps >= 1, "the table must retain at least the current snapshot"


def test_files_sorted_internally(submission, spark_iceberg):
    spark = spark_iceberg
    _run(submission, spark)
    # Reading a file preserves write order; with WRITE ORDERED BY sort_col each
    # data file is clustered, so values are non-decreasing within every file.
    rows = spark.sql(
        f"SELECT _file, {SORT_COL} FROM {TABLE}"
    ).collect()
    per_file = {}
    for r in rows:
        per_file.setdefault(r["_file"], []).append(r[SORT_COL])
    for f, vals in per_file.items():
        assert vals == sorted(vals), f"file {f} is not sorted by {SORT_COL}"
