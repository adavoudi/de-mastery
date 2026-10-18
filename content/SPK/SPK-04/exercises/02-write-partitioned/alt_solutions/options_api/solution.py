"""SPK-04 / 02-write-partitioned — alt: configure the writer via options/mode enum."""


def write_partitioned(df, out_path):
    writer = df.write.partitionBy("company_id").option("compression", "snappy")
    writer.mode("overwrite").parquet(out_path)
