"""Mutant: no deduplication — every duplicate row is appended."""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP"


def dedup(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    deduped = stream.withWatermark("event_ts", "30 minutes")  # WRONG: no dedup op
    query = (
        deduped.writeStream
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.processAllAvailable()
    query.stop()
