"""Tests for SPK-12 / 02-project-config.

Checks config-precedence rules (BASE always present; dev/prod profiles; overrides win;
no secrets baked in) and that revenue_report matches a DuckDB oracle.
"""

from de_testkit import assert_frames_equal

BASE_KEYS = {
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.session.timeZone",
}

ORDERS = [
    ("C1", 1, 100.0), ("C1", 1, 100.0), ("C1", 2, 50.0),
    ("C2", 3, 200.0), ("C3", 4, 10.0),
]


def test_base_keys_always_present(submission):
    for env in ("dev", "prod", "unknown"):
        conf = submission.build_config(env)
        assert BASE_KEYS <= set(conf), (env, set(conf))
        assert conf["spark.sql.session.timeZone"] == "UTC"


def test_dev_profile(submission):
    conf = submission.build_config("dev")
    assert conf["spark.sql.shuffle.partitions"] == "8"
    assert conf["spark.sql.adaptive.enabled"] == "false"


def test_prod_profile(submission):
    conf = submission.build_config("prod")
    assert conf["spark.sql.adaptive.enabled"] == "true"
    assert int(conf["spark.sql.shuffle.partitions"]) > int(
        submission.build_config("dev")["spark.sql.shuffle.partitions"])


def test_overrides_win(submission):
    conf = submission.build_config(
        "prod", overrides={"spark.sql.shuffle.partitions": "17",
                           "spark.sql.adaptive.enabled": "false"})
    assert conf["spark.sql.shuffle.partitions"] == "17"
    assert conf["spark.sql.adaptive.enabled"] == "false"


def test_no_secrets_in_defaults(submission):
    # BASE/dev/prod must not contain secret-looking keys/values (secrets come via overrides).
    bad = ("password", "secret", "token", "aws_", "access_key", "/home/", "s3://")
    for env in ("dev", "prod"):
        for k, v in submission.build_config(env).items():
            blob = f"{k}={v}".lower()
            assert not any(b in blob for b in bad), (env, k, v)


def test_revenue_report_matches_oracle(submission, spark, duckdb_con):
    orders = spark.createDataFrame(
        ORDERS, "company_id STRING, order_item_id INT, line_amount DOUBLE")
    out = submission.revenue_report(orders)
    assert set(out.columns) >= {"company_id", "revenue"}
    duckdb_con.execute(
        "CREATE TABLE o AS SELECT * FROM (VALUES " +
        ",".join(f"('{c}', {i}, {a})" for c, i, a in ORDERS) +
        ") AS t(company_id, order_item_id, line_amount)")
    oracle = duckdb_con.execute(
        "SELECT company_id, sum(line_amount) AS revenue FROM o GROUP BY company_id").arrow()
    assert_frames_equal(out.select("company_id", "revenue"), oracle, rel_tol=1e-6)
