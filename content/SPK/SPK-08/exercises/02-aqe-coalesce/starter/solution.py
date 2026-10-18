"""SPK-08 / 02-aqe-coalesce — starter. See README.md."""


def coalesced_aggregate(spark, order_items):
    # TODO: enable AQE + coalescePartitions, set a high shuffle.partitions and
    # minPartitionNum=1, then return per-company sum(line_amount) as
    # company_id, total. The final plan must show AQE coalescing.
    raise NotImplementedError
