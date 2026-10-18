"""SPK-11 / 02-dedup-within-watermark — alternate: explicit iceberg format sink."""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP"


def dedup(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    deduped = (
        stream
        .withWatermark("event_ts", "30 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    query = (
        deduped.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", table)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.processAllAvailable()
    query.stop()
