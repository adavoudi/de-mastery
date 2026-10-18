"""SPK-12 / 01-test-the-pipeline — starter. See README.md.

Write assert_valid so it PASSES a correct per-company revenue result and RAISES
AssertionError on a wrong one. This starter is too weak: it only checks the row count,
so it fails to catch most bugs.
"""


def assert_valid(spark, result_df, orders_df):
    # TOO WEAK: only checks that there is at least one row. Replace with a real oracle
    # comparison plus invariants (see README.md).
    assert result_df.count() >= 1
