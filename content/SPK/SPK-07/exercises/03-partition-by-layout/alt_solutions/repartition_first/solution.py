"""SPK-07 / 03 — alt: repartition by company_id first (so each company's rows are
colocated into one task → one file per partition dir), then partitionBy. Same
on-disk company_id=.../ layout, fewer files per directory."""


def write_partitioned(df, out_dir):
    out = str(out_dir)
    (
        df.repartition("company_id")
        .write.mode("overwrite")
        .partitionBy("company_id")
        .parquet(out)
    )
    return out
