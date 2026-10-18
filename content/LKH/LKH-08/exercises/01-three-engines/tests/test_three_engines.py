"""Tests for LKH-08 / 01-three-engines.

Graded (no Docker): Spark over an Iceberg table and DuckDB over the same rows as
Hive-partitioned Parquet must return identical results for the SAME query (two
independent engines = oracle, G6). A second graded part checks the learner's
plan_facts.yaml against the TRUTH extracted from Spark's real physical plan.

Optional: if the `core` lab is up, the same query on Trino must agree with Spark.
That test SKIPS cleanly when the lab is down (never fails), so it is a bonus layer.
"""

import os
from pathlib import Path

import yaml

TABLE = "local.lkh08.sales"
N = 200


def _seed(spark, tmp_path):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lkh08")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(
        f"CREATE TABLE {TABLE} (id INT, region STRING, cat STRING, amt DOUBLE) "
        f"USING iceberg PARTITIONED BY (region)"
    )
    vals = ",".join(
        f"({i}, 'r{i % 4}', 'c{i % 3}', {float(i)})" for i in range(N)
    )
    spark.sql(f"INSERT INTO {TABLE} VALUES {vals}")
    # Same rows as a Hive-partitioned Parquet copy for DuckDB.
    pq = str(tmp_path / "pq")
    spark.table(TABLE).write.partitionBy("region").mode("overwrite").parquet(pq)
    return pq + "/*/*.parquet"


def _rows(result):
    """Normalize a Spark DF or Arrow table to a sorted list of (cat, round(total))."""
    if hasattr(result, "collect"):  # Spark DataFrame
        return sorted((r["cat"], round(r["total"], 4)) for r in result.collect())
    tbl = result.to_pylist() if hasattr(result, "to_pylist") else result
    return sorted((r["cat"], round(r["total"], 4)) for r in tbl)


def test_spark_and_duckdb_agree(submission, spark_iceberg, duckdb_con, tmp_path):
    spark = spark_iceberg
    glob = _seed(spark, tmp_path)
    spark_res = _rows(submission.run_spark(spark, TABLE))
    duck_res = _rows(submission.run_duckdb(duckdb_con, glob))
    assert spark_res, "run_spark returned no rows"
    assert spark_res == duck_res, (
        f"engines disagree:\n  spark={spark_res}\n  duckdb={duck_res}"
    )


def test_result_is_correct(submission, spark_iceberg, duckdb_con, tmp_path):
    # Independent recompute of the expected aggregate (oracle), via DuckDB on the
    # raw rows the test generated — not via the learner's query.
    spark = spark_iceberg
    _seed(spark, tmp_path)
    raw = [
        {"id": i, "region": f"r{i % 4}", "cat": f"c{i % 3}", "amt": float(i)}
        for i in range(N)
    ]
    duckdb_con.register("raw", _arrow(raw))
    oracle = sorted(
        (r[0], round(r[1], 4))
        for r in duckdb_con.execute(
            "SELECT cat, SUM(amt) FROM raw WHERE region='r1' AND amt>50 GROUP BY cat"
        ).fetchall()
    )
    got = _rows(submission.run_spark(spark, TABLE))
    assert got == oracle, f"query result wrong: got {got}, expected {oracle}"


def _arrow(rows):
    import pyarrow as pa

    return pa.Table.from_pylist(rows)


def _iceberg_scan_filters(df):
    """Return the lowercased `filters=...` text from the Iceberg BatchScan plan."""
    plan = df._jdf.queryExecution().explainString(
        df.sparkSession._jvm.org.apache.spark.sql.execution.ExplainMode.fromString(
            "formatted"
        )
    )
    low = plan.lower()
    # The Iceberg scan prints e.g. "... [filters=region IS NOT NULL, amt > 50.0, ...]"
    out = []
    for line in low.splitlines():
        if "filters=" in line:
            out.append(line)
    return "\n".join(out), low


def _plan_truth(df):
    filters_text, full = _iceberg_scan_filters(df)
    data_pushed = "amt > 50" in filters_text
    partition_pushed = "region = 'r1'" in filters_text or "region = r1" in filters_text
    return {"data_filter_pushed": data_pushed, "partition_filter_pushed": partition_pushed}


def _claims():
    path = Path(os.environ["DE_SUBMISSION_DIR"]) / "plan_facts.yaml"
    with open(path) as f:
        return yaml.safe_load(f) or {}


def test_plan_facts_match_reality(submission, spark_iceberg, tmp_path):
    spark = spark_iceberg
    _seed(spark, tmp_path)
    df = submission.run_spark(spark, TABLE)
    truth = _plan_truth(df)
    claims = _claims()
    for key, actual in truth.items():
        assert key in claims, f"plan_facts.yaml missing {key}"
        assert isinstance(claims[key], bool), f"{key} must be true/false, not {claims[key]!r}"
        assert claims[key] == actual, (
            f"plan_facts.yaml[{key}]={claims[key]} but the real plan shows {actual}"
        )
    # This query genuinely pushes both predicates; guard against a vacuous all-false.
    assert truth["data_filter_pushed"] and truth["partition_filter_pushed"], (
        "sanity: expected both predicates pushed for this query/table"
    )


def test_trino_agrees_if_lab_up(submission, spark_iceberg, duckdb_con, trino, tmp_path):
    # Optional bonus: SKIPS cleanly (via the `trino` fixture) when the core lab is
    # down. When the lab is up, Trino runs the SAME query against a self-contained
    # table it creates in the lab's `iceberg` catalog, and must agree with the
    # DuckDB oracle over the same rows. We create and drop our own table so this is
    # independent of the local (no-Docker) `local` catalog used above.
    _ = spark_iceberg  # ensures the Iceberg fixture is active (skips w/o JARs)
    oracle = sorted(
        (r[0], round(r[1], 4))
        for r in duckdb_con.execute(
            "SELECT cat, SUM(amt) FROM (SELECT * FROM (VALUES "
            + ",".join(
                f"({i}, 'r{i % 4}', 'c{i % 3}', {float(i)})" for i in range(N)
            )
            + ") AS t(id, region, cat, amt)) WHERE region='r1' AND amt>50 GROUP BY cat"
        ).fetchall()
    )
    cur = trino.cursor()
    schema = "de_lkh08_tmp"
    tbl = f"iceberg.{schema}.sales_probe"
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS iceberg.{schema}")
        cur.fetchall()
        cur.execute(f"DROP TABLE IF EXISTS {tbl}")
        cur.fetchall()
        cur.execute(
            f"CREATE TABLE {tbl} (id INTEGER, region VARCHAR, cat VARCHAR, amt DOUBLE) "
            f"WITH (partitioning = ARRAY['region'])"
        )
        cur.fetchall()
        values = ",".join(
            f"({i}, 'r{i % 4}', 'c{i % 3}', CAST({float(i)} AS DOUBLE))"
            for i in range(N)
        )
        cur.execute(f"INSERT INTO {tbl} VALUES {values}")
        cur.fetchall()
        cur.execute(
            f"SELECT cat, SUM(amt) AS total FROM {tbl} "
            f"WHERE region = 'r1' AND amt > 50 GROUP BY cat"
        )
        trino_res = sorted((r[0], round(r[1], 4)) for r in cur.fetchall())
        assert trino_res == oracle, f"trino={trino_res} oracle={oracle}"
    finally:
        try:
            cur.execute(f"DROP TABLE IF EXISTS {tbl}")
            cur.fetchall()
        except Exception:
            pass
