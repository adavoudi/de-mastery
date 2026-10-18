"""Mutant: overrides are applied FIRST, so the env profile overwrites them (wrong precedence)."""

from pyspark.sql import functions as F

_BASE = {
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.session.timeZone": "UTC",
}
_PROFILES = {
    "dev": {"spark.sql.shuffle.partitions": "8"},
    "prod": {"spark.sql.adaptive.enabled": "true", "spark.sql.shuffle.partitions": "400"},
}


def build_config(env, overrides=None):
    conf = dict(_BASE)
    conf.update(overrides or {})          # WRONG order
    conf.update(_PROFILES.get(env, {}))   # profile wins over overrides
    return conf


def revenue_report(orders_df):
    return orders_df.groupBy("company_id").agg(F.sum("line_amount").alias("revenue"))
