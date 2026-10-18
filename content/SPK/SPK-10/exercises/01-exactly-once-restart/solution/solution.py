"""SPK-10 / 01-exactly-once-restart — reference solution.

Native table sink + reused checkpoint + availableNow = exactly-once across restarts.
"""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts STRING"


def run_batch(spark, source_dir, table, checkpoint):
    stream = (
        spark.readStream
        .schema(SCHEMA)
        .format("json")
        .load(source_dir)
    )
    query = (
        stream.writeStream
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.processAllAvailable()
    query.stop()
