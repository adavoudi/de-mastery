"""SPK-04 / 02-write-partitioned — alt: the generic format/save API."""


def write_partitioned(df, out_path):
    (df.write
       .format("parquet")
       .mode("overwrite")
       .partitionBy("company_id")
       .save(out_path))
