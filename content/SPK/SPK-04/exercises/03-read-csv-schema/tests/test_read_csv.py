"""Tests for SPK-04 / 03-read-csv-schema.

Oracle: the Parquet version of the same table is the typed source of truth. The CSV read,
done with an explicit schema and nullValue='', must match it exactly.
"""

from de_testkit import assert_frames_equal


def test_matches_parquet_source_of_truth(submission, spark, commerce_data):
    csv_path = str(commerce_data["csv"]["customers"])
    parquet = spark.read.parquet(str(commerce_data["parquet"]["customers"]))
    out = submission.read_customers_csv(spark, csv_path)
    assert_frames_equal(out, parquet)


def test_customer_id_is_integral(submission, spark, commerce_data):
    out = submission.read_customers_csv(spark, str(commerce_data["csv"]["customers"]))
    dt = dict(out.dtypes)["customer_id"]
    assert dt in ("bigint", "int", "long"), f"customer_id typed as {dt}"


def test_empty_email_is_null(submission, spark, commerce_data):
    out = submission.read_customers_csv(spark, str(commerce_data["csv"]["customers"]))
    parquet = spark.read.parquet(str(commerce_data["parquet"]["customers"]))
    # same number of NULL emails as the parquet truth; and no empty-string emails
    assert out.filter(out["email"].isNull()).count() == \
        parquet.filter(parquet["email"].isNull()).count()
    assert out.filter(out["email"] == "").count() == 0


def test_row_count_excludes_header(submission, spark, commerce_data):
    out = submission.read_customers_csv(spark, str(commerce_data["csv"]["customers"]))
    parquet = spark.read.parquet(str(commerce_data["parquet"]["customers"]))
    assert out.count() == parquet.count()
