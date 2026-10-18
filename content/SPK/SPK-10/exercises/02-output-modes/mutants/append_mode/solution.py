"""Mutant: append output mode on a running (unwatermarked) aggregation (invalid)."""

from pyspark.sql import functions as F

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts STRING"


def run_counts(spark, source_dir, query_name):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    agg = stream.groupBy("company_id").agg(F.count(F.lit(1)).alias("n"))
    query = (
        agg.writeStream
        .outputMode("append")  # WRONG: append is illegal for this aggregation
        .format("memory")
        .queryName(query_name)
        .trigger(availableNow=True)
        .start()
    )
    query.processAllAvailable()
    query.stop()
    return query
