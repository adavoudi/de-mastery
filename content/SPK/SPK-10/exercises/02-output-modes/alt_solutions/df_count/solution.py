"""SPK-10 / 02-output-modes — alternate: count the key column, grouped via DataFrame.count.

Uses DataFrame.groupBy(...).count() (column auto-named "count", then renamed to n)
instead of an explicit agg expression. Still complete mode on the memory sink.
"""

SCHEMA = "event_id INT, company_id STRING, amount DOUBLE, event_ts STRING"


def run_counts(spark, source_dir, query_name):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    agg = stream.groupBy("company_id").count().withColumnRenamed("count", "n")
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
