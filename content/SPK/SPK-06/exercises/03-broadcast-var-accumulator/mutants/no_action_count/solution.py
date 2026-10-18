"""Mutant: reads the accumulator BEFORE any action runs. The filter is lazy, so
the accumulator is still 0 when returned — invalid_count is wrong (0)."""


def validate_currencies(spark, orders, valid_currencies):
    sc = spark.sparkContext
    allow = sc.broadcast(set(valid_currencies))
    bad = sc.accumulator(0)
    schema = orders.schema

    def keep(row):
        if row["currency"] in allow.value:
            return True
        bad.add(1)
        return False

    valid_rdd = orders.rdd.filter(keep)  # lazy, not materialized
    # Build the DataFrame lazily too and read the accumulator without an action.
    valid_df = valid_rdd.toDF(schema)
    return valid_df, bad.value  # bad.value is still 0 here
