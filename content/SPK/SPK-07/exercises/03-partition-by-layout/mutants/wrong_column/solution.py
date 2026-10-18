"""Mutant: partitions by currency instead of company_id, so the directories are
currency=<value>/ (including bad codes) and there is no company_id=<value>/ layout
with one dir per company."""


def write_partitioned(df, out_dir):
    out = str(out_dir)
    df.write.mode("overwrite").partitionBy("currency").parquet(out)
    return out
