"""LKH-04 / 01-scd2-merge — alt: DataFrame rebuild + overwritePartitions.

Computes the next dimension state with the DataFrame API (history + closed +
unchanged + new versions) and overwrites the table. No SQL MERGE at all.
"""

from pyspark.sql import functions as F


def apply_scd2(spark, dim_ident, src_values_sql, batch_date):
    src = spark.sql(src_values_sql).select("nk", "attr")
    full = spark.table(dim_ident)
    cur = full.filter("is_current = true")
    hist = full.filter("is_current = false")

    joined = cur.alias("c").join(src.alias("u"), "nk", "left")
    bd = F.to_date(F.lit(batch_date))

    # current rows whose attr changed -> close them
    closed = (
        joined.filter("u.attr IS NOT NULL AND c.attr <> u.attr")
        .select("c.nk", "c.attr", "c.valid_from")
        .withColumn("valid_to", bd)
        .withColumn("is_current", F.lit(False))
    )
    # current rows that did not change -> keep unchanged
    unchanged = joined.filter("u.attr IS NULL OR c.attr = u.attr").select(
        "c.nk", "c.attr", "c.valid_from", "c.valid_to", "c.is_current"
    )
    # new open versions: source keys not currently current, plus changed keys
    new_keys = src.join(cur.select("nk"), "nk", "left_anti")
    changed_keys = (
        src.alias("u").join(cur.alias("c"), "nk").filter("c.attr <> u.attr")
        .select("u.nk", "u.attr")
    )
    new_versions = (
        new_keys.unionByName(changed_keys)
        .select("nk", "attr")
        .withColumn("valid_from", bd)
        .withColumn("valid_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
    )

    result = hist.unionByName(closed).unionByName(unchanged).unionByName(new_versions)
    result.writeTo(dim_ident).overwritePartitions()
