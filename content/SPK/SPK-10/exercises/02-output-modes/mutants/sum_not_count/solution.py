"""Mutant: n is the sum of amount instead of the event count."""

from pyspark.sql import functions as F

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts STRING"


def run_counts(spark, source_dir, query_name):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    agg = stream.groupBy("company_id").agg(F.sum("amount").alias("n"))  # WRONG: sum, not count
    query = (
        agg.writeStream
        .outputMode("complete")
        .format("memory")
        .queryName(query_name)
        .trigger(availableNow=True)
        .start()
    )
    query.processAllAvailable()
    query.stop()
    return query
