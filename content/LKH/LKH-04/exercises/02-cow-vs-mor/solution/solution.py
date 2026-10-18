"""LKH-04 / 02-cow-vs-mor — reference solution.

Create a v2 table with the chosen row-level mode, and read the .files metadata
table to expose data vs delete files.
"""


def create_table(spark, ident, mode):
    assert mode in ("copy-on-write", "merge-on-read")
    spark.sql(
        f"""
        CREATE TABLE {ident} (id INT, cc STRING) USING iceberg
        TBLPROPERTIES (
          'format-version' = '2',
          'write.delete.mode' = '{mode}',
          'write.update.mode' = '{mode}',
          'write.merge.mode'  = '{mode}'
        )
        """
    )


def files_by_content(spark, ident):
    rows = spark.sql(
        f"SELECT content, count(*) AS c FROM {ident}.files GROUP BY content"
    ).collect()
    return {int(r["content"]): int(r["c"]) for r in rows}


def data_file_paths(spark, ident):
    rows = spark.sql(
        f"SELECT file_path FROM {ident}.files WHERE content = 0"
    ).collect()
    return {r["file_path"] for r in rows}
