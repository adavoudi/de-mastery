"""LKH-04 / 02-cow-vs-mor — alt: ALTER SET TBLPROPERTIES + split metadata tables.

Creates the v2 table then sets the modes with ALTER, and reads the dedicated
.data_files / .delete_files metadata tables instead of grouping .files by content.
"""


def create_table(spark, ident, mode):
    assert mode in ("copy-on-write", "merge-on-read")
    spark.sql(
        f"CREATE TABLE {ident} (id INT, cc STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '2')"
    )
    spark.sql(
        f"ALTER TABLE {ident} SET TBLPROPERTIES ("
        f"'write.delete.mode' = '{mode}', "
        f"'write.update.mode' = '{mode}', "
        f"'write.merge.mode'  = '{mode}')"
    )


def files_by_content(spark, ident):
    out = {}
    data = spark.sql(f"SELECT count(*) AS c FROM {ident}.data_files").collect()[0]["c"]
    if data:
        out[0] = int(data)
    # delete_files holds both position (1) and equality (2) deletes; split by content.
    rows = spark.sql(
        f"SELECT content, count(*) AS c FROM {ident}.delete_files GROUP BY content"
    ).collect()
    for r in rows:
        out[int(r["content"])] = int(r["c"])
    return out


def data_file_paths(spark, ident):
    rows = spark.sql(f"SELECT file_path FROM {ident}.data_files").collect()
    return {r["file_path"] for r in rows}
