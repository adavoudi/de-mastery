"""SPK-07 / 01 — alt: cap records per file. Collapse to one partition, then let
Spark split that partition into exactly num_files files via maxRecordsPerFile =
ceil(rows / num_files). Different mechanism (size/record cap, no key shuffle)."""

import math


def write_balanced(df, out_dir, num_files):
    out = str(out_dir)
    rows = df.count()
    cap = max(1, math.ceil(rows / num_files))
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("maxRecordsPerFile", cap)
        .parquet(out)
    )
    return out
