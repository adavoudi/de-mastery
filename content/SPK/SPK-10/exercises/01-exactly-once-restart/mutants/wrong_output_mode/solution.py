"""Mutant: complete output mode on a non-aggregating stream (invalid)."""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts STRING"


def run_batch(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    query = (
        stream.writeStream
        .outputMode("complete")  # WRONG: complete is only valid for aggregations
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.processAllAvailable()
    query.stop()
