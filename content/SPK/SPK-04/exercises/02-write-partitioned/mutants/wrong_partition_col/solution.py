"""Mutant: partitions by order_id (high cardinality, wrong column)."""


def write_partitioned(df, out_path):
    # WRONG: partitions by a high-cardinality key, so there are no company_id dirs
    # and a directory explosion (the small-file problem).
    df.write.mode("overwrite").partitionBy("order_id").parquet(out_path)
