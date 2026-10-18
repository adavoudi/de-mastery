"""Mutant: windows on processing time (current_timestamp) instead of event time."""

from pyspark.sql import functions as F

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP"


def windowed_counts(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    agg = (
        stream
        .withColumn("proc_ts", F.current_timestamp())  # WRONG: processing time
        .withWatermark("proc_ts", "10 minutes")
        .groupBy(F.window("proc_ts", "5 minutes"), "company_id")
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
