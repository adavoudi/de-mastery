"""SPK-10 / 01-exactly-once-restart — alternate: explicit iceberg format + path.

Same exactly-once behavior expressed via .format("iceberg").option("path", table)
and .start() instead of the toTable(...) convenience.
"""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts STRING"


def run_batch(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    query = (
        stream.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", table)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.processAllAvailable()
    query.stop()
