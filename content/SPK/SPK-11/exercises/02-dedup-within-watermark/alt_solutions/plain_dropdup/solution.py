"""SPK-11 / 02-dedup-within-watermark — alternate: plain dropDuplicates (still watermarked).

dropDuplicates(['event_id']) with a watermark produces the same dedup result for data
within the horizon; it only differs in that it would keep state unbounded over a long
run. The test asserts the dedup RESULT, not which operator was used.
"""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP"


def dedup(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    deduped = (
        stream
        .withWatermark("event_ts", "30 minutes")
        .dropDuplicates(["event_id"])
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
