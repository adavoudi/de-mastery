"""Mutant: dedups on company_id, collapsing genuinely distinct events."""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP"


def dedup(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    deduped = (
        stream
        .withWatermark("event_ts", "30 minutes")
        .dropDuplicatesWithinWatermark(["company_id"])  # WRONG key: keeps 1 row per company
    )
    query = (
        deduped.writeStream
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.processAllAvailable()
    query.stop()
