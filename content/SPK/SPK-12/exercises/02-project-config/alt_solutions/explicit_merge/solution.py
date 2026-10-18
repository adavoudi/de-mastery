"""SPK-12 / 02-project-config — alternate: explicit per-key merge + expr transform."""

from pyspark.sql import functions as F


def _base():
    return {
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.session.timeZone": "UTC",
    }


def build_config(env, overrides=None):
    conf = _base()
    if env == "dev":
        conf["spark.sql.shuffle.partitions"] = "8"
    elif env == "prod":
        conf["spark.sql.adaptive.enabled"] = "true"
        conf["spark.sql.shuffle.partitions"] = "400"
    for k, v in (overrides or {}).items():
        conf[k] = v
    return conf


def revenue_report(orders_df):
    return (orders_df.groupBy("company_id")
            .agg(F.expr("sum(line_amount)").alias("revenue")))
