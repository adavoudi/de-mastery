"""Mutant: keeps the INVALID rows (allowlist logic inverted). valid_df is wrong."""


def validate_currencies(spark, orders, valid_currencies):
    sc = spark.sparkContext
    allow = sc.broadcast(set(valid_currencies))
    bad = sc.accumulator(0)
    schema = orders.schema

    def keep(row):
        if row["currency"] not in allow.value:  # inverted
            bad.add(1)
            return True
        return False

    rows = orders.rdd.filter(keep).collect()
    return spark.createDataFrame(rows, schema=schema), bad.value
