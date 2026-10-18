"""SPK-11 / 01-watermark-window — alternate: foreachBatch append of finalized windows.

The windowed aggregation is identical (append mode, watermark finalizes each window
once); the sink is a foreachBatch that appends the finalized rows via DataFrameWriterV2.
"""

from pyspark.sql import functions as F

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP"


def windowed_counts(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    agg = (
        stream
        .withWatermark("event_ts", "10 minutes")
        .groupBy(F.window("event_ts", "5 minutes"), "company_id")
        .count()
        .select("company_id", F.col("window.start").alias("w_start"),
                F.col("count").alias("n"))
    )

    def append_batch(batch_df, batch_id):
        batch_df.writeTo(table).append()

    query = (
        agg.writeStream
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .foreachBatch(append_batch)
        .start()
    )
    query.processAllAvailable()
    query.stop()
