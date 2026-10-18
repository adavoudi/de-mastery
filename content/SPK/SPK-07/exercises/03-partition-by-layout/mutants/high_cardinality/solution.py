"""Mutant: partitions by the high-cardinality order_id — one directory per order,
the small-file explosion. The partition-dir count is the number of orders, not the
number of companies, and the dirs are order_id=.../ not company_id=.../."""


def write_partitioned(df, out_dir):
    out = str(out_dir)
    df.write.mode("overwrite").partitionBy("order_id").parquet(out)
    return out
