"""Mutant: increments the accumulator for VALID rows, so invalid_count ends up as
the valid-row count, not the invalid-row count."""


def validate_currencies(spark, orders, valid_currencies):
    sc = spark.sparkContext
    allow = sc.broadcast(set(valid_currencies))
    bad = sc.accumulator(0)
    schema = orders.schema

    def keep(row):
        if row["currency"] in allow.value:
            bad.add(1)  # wrong: counts valid rows
            return True
        return False

    rows = orders.rdd.filter(keep).collect()
    return spark.createDataFrame(rows, schema=schema), bad.value
