"""SPK-07 / 03 — alt: partitionBy with a list arg (equivalent layout)."""


def write_partitioned(df, out_dir):
    out = str(out_dir)
    df.write.partitionBy(["company_id"]).mode("overwrite").format("parquet").save(out)
    return out
