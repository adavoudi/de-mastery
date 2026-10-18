"""SPK-11 / 01-watermark-window — alternate: explicit agg expression + iceberg format sink."""

from pyspark.sql import functions as F

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP"


def windowed_counts(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    agg = (
        stream
        .withWatermark("event_ts", "10 minutes")
        .groupBy(F.window("event_ts", "5 minutes"), "company_id")
        .agg(F.count(F.lit(1)).alias("n"))
        .select("company_id", F.col("window.start").alias("w_start"), "n")
    )
    query = (
        agg.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", table)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.processAllAvailable()
    query.stop()
