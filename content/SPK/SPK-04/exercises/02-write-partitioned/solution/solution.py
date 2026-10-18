"""SPK-04 / 02-write-partitioned — reference solution."""


def write_partitioned(df, out_path):
    (df.write
       .mode("overwrite")
       .partitionBy("company_id")
       .parquet(out_path))
