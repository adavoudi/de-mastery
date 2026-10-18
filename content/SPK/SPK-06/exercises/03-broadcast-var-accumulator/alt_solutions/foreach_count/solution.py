"""SPK-06 / 03 — alt: use foreach (an action) + the accumulator purely to count
invalid rows, and build valid_df with a broadcast-driven filter separately."""


def validate_currencies(spark, orders, valid_currencies):
    sc = spark.sparkContext
    allow = sc.broadcast(set(valid_currencies))
    bad = sc.accumulator(0)

    def count_bad(row):
        if row["currency"] not in allow.value:
            bad.add(1)

    # foreach is an action, so the accumulator is populated here.
    orders.rdd.foreach(count_bad)

    valid_df = orders.rdd.filter(
        lambda r: r["currency"] in allow.value
    ).toDF(orders.schema)
    return valid_df, bad.value
