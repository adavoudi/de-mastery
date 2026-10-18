"""SPK-06 / 03-broadcast-var-accumulator — reference solution."""


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

    valid_rdd = orders.rdd.filter(keep)
    # Materialize the RDD so the accumulator is populated, then rebuild a DataFrame.
    rows = valid_rdd.collect()
    valid_df = spark.createDataFrame(rows, schema=schema)
    return valid_df, bad.value
