"""SPK-10 / 02-output-modes — alternate: update mode.

update is also valid for an aggregation: each batch emits the rows that changed. In
the memory sink the table holds the latest value per key, so after draining all input
it holds the full current counts — the same final state as complete.
"""

from pyspark.sql import functions as F

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts STRING"


def run_counts(spark, source_dir, query_name):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    agg = stream.groupBy("company_id").agg(F.count(F.lit(1)).alias("n"))
    query = (
        agg.writeStream
        .outputMode("update")
        .format("memory")
        .queryName(query_name)
        .trigger(availableNow=True)
        .start()
    )
    query.processAllAvailable()
    query.stop()
    return query
