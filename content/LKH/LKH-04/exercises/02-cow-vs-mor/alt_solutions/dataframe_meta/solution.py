"""LKH-04 / 02-cow-vs-mor — alt: DataFrame API over the .files metadata table.

Reads the metadata table as a DataFrame (spark.table) and aggregates with the
DataFrame API rather than a SQL GROUP BY. Same facts, different surface.
"""

from pyspark.sql import functions as F


def create_table(spark, ident, mode):
    assert mode in ("copy-on-write", "merge-on-read")
    props = ", ".join(
        [
            "'format-version' = '2'",
            f"'write.delete.mode' = '{mode}'",
            f"'write.update.mode' = '{mode}'",
            f"'write.merge.mode'  = '{mode}'",
        ]
    )
    spark.sql(f"CREATE TABLE {ident} (id INT, cc STRING) USING iceberg TBLPROPERTIES ({props})")


def files_by_content(spark, ident):
    df = spark.table(f"{ident}.files")
    agg = df.groupBy("content").agg(F.count(F.lit(1)).alias("c")).collect()
    return {int(r["content"]): int(r["c"]) for r in agg}


def data_file_paths(spark, ident):
    df = spark.table(f"{ident}.files").filter(F.col("content") == 0)
    return {r["file_path"] for r in df.select("file_path").collect()}
