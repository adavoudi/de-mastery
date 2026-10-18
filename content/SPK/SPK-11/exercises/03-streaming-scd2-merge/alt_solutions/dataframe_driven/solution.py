"""SPK-11 / 03-streaming-scd2-merge — alternate: DataFrame-driven inserts.

Closes changed rows with a MERGE, but computes the new open rows with the DataFrame
API (join the batch to the current open versions, keep new/changed keys) and appends
them with writeTo(...).append(). A genuinely different route from the two-SQL-pass
reference, with the same SCD2 result.
"""

from pyspark.sql import functions as F

SCHEMA = "cust_key INT, attr STRING, change_ts TIMESTAMP"


def _apply(table):
    def fn(batch_df, batch_id):
        spark = batch_df.sparkSession
        changes = batch_df.dropDuplicates(["cust_key"])
        changes.createOrReplaceTempView("scd2_changes")

        current = (spark.table(table).filter("is_current = true")
                   .select(F.col("cust_key").alias("_ck"),
                           F.col("attr").alias("_cur_attr")))
        joined = changes.join(current, changes["cust_key"] == F.col("_ck"), "left")
        new_versions = joined.filter(F.col("_ck").isNull() | (F.col("_cur_attr") != F.col("attr")))

        # Close the current open row where the attribute changed.
        spark.sql(f"""
            MERGE INTO {table} t
            USING scd2_changes c
            ON t.cust_key = c.cust_key AND t.is_current = true
            WHEN MATCHED AND t.attr <> c.attr
              THEN UPDATE SET t.valid_to = c.change_ts, t.is_current = false
        """)

        inserts = new_versions.select(
            F.col("cust_key"),
            F.col("attr"),
            F.col("change_ts").alias("valid_from"),
            F.lit(None).cast("timestamp").alias("valid_to"),
            F.lit(True).alias("is_current"),
        )
        inserts.writeTo(table).append()
    return fn


def upsert_scd2(spark, source_dir, table, checkpoint):
    stream = spark.readStream.schema(SCHEMA).format("json").load(source_dir)
    query = (
        stream.writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .foreachBatch(_apply(table))
        .start()
    )
    query.processAllAvailable()
    query.stop()
