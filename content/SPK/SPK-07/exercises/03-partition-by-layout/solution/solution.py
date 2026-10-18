"""SPK-07 / 03-partition-by-layout — reference solution."""


def write_partitioned(df, out_dir):
    out = str(out_dir)
    df.write.mode("overwrite").partitionBy("company_id").parquet(out)
    return out
