"""SPK-10 / 02-output-modes — reference solution (complete mode).

An aggregation can use complete (whole result each batch) or update. complete leaves
the full current result queryable in the memory sink, which is what the dashboard reads.
"""

from pyspark.sql import functions as F

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts STRING"


def run_counts(spark, source_dir, query_name):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    agg = stream.groupBy("company_id").agg(F.count(F.lit(1)).alias("n"))
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
