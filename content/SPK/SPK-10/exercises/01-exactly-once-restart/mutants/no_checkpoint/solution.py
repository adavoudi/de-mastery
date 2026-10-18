"""Mutant: no checkpointLocation, so each run forgets prior progress."""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts STRING"


def run_batch(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    query = (
        stream.writeStream
        .outputMode("append")
        .trigger(availableNow=True)  # WRONG: no checkpointLocation -> no durable offsets
        .toTable(table)
    )
    query.processAllAvailable()
    query.stop()
