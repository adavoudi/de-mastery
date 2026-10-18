"""Mutant: dropDuplicatesWithinWatermark with no watermark declared (invalid)."""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP"


def dedup(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    # WRONG: dropDuplicatesWithinWatermark requires a watermark; none is set.
    deduped = stream.dropDuplicatesWithinWatermark(["event_id"])
    query = (
        deduped.writeStream
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.processAllAvailable()
    query.stop()
