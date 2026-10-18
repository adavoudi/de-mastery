"""Mutant: no watermark + update mode, so late events are still counted (window never finalizes/drops)."""

from pyspark.sql import functions as F

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP"


def windowed_counts(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    agg = (
        stream
        # WRONG: no withWatermark -> late data is never dropped
        .groupBy(F.window("event_ts", "5 minutes"), "company_id")
        .count()
        .select("company_id", F.col("window.start").alias("w_start"),
                F.col("count").alias("n"))
    )
    query = (
        agg.writeStream
        .outputMode("complete")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .foreachBatch(lambda b, i: b.writeTo(table).createOrReplace())
        .start()
    )
    query.processAllAvailable()
    query.stop()
