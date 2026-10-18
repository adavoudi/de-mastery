"""Mutant: 10-minute windows instead of 5-minute, so window boundaries/counts differ."""

from pyspark.sql import functions as F

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP"


def windowed_counts(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    agg = (
        stream
        .withWatermark("event_ts", "10 minutes")
        .groupBy(F.window("event_ts", "10 minutes"), "company_id")  # WRONG size
        .count()
        .select("company_id", F.col("window.start").alias("w_start"),
                F.col("count").alias("n"))
    )
    query = (
        agg.writeStream
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.processAllAvailable()
    query.stop()
