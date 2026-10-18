"""SPK-06 / 03 — alt: mapPartitions reads the broadcast once per partition and
updates the accumulator, yielding kept rows."""


def validate_currencies(spark, orders, valid_currencies):
    sc = spark.sparkContext
    allow = sc.broadcast(set(valid_currencies))
    bad = sc.accumulator(0)
    schema = orders.schema

    def process(part):
        allowed = allow.value
        for row in part:
            if row["currency"] in allowed:
                yield row
            else:
                bad.add(1)

    kept = orders.rdd.mapPartitions(process).collect()
    return spark.createDataFrame(kept, schema=schema), bad.value
