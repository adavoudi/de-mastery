"""SPK-12 / 02-project-config — alternate: ChainMap precedence + SQL transform."""

from collections import ChainMap

BASE = {
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.session.timeZone": "UTC",
}
PROFILES = {
    "dev": {"spark.sql.shuffle.partitions": "8"},
    "prod": {"spark.sql.adaptive.enabled": "true", "spark.sql.shuffle.partitions": "400"},
}


def build_config(env, overrides=None):
    # ChainMap resolves left-to-right; wrap in dict() to materialize a flat mapping.
    return dict(ChainMap(overrides or {}, PROFILES.get(env, {}), BASE))


def revenue_report(orders_df):
    spark = orders_df.sparkSession
    orders_df.createOrReplaceTempView("o_spk12_02")
    return spark.sql(
        "SELECT company_id, sum(line_amount) AS revenue FROM o_spk12_02 GROUP BY company_id"
    )
