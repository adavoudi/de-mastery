"""Mutant: partition with Python's built-in hash(), which is salted per process
(PYTHONHASHSEED), so the mapping is not stable across processes."""


def message_key(event):
    return str(event["order_id"])


def partition_for(event, num_partitions):
    # BUG: hash() of a str is randomized per interpreter -> non-deterministic.
    return hash(message_key(event)) % num_partitions


def design():
    return {
        "orders": {
            "topic_type": "event",
            "retention": "time",
            "key": "order_id",
            "factors": [
                "per_entity_ordering",
                "avoid_hot_partition",
                "high_cardinality_key",
                "append_only_history",
            ],
        },
        "customers": {
            "topic_type": "entity",
            "retention": "compacted",
            "key": "customer_id",
            "factors": [
                "latest_state_per_key",
                "log_compaction",
                "rebuildable_from_log",
            ],
        },
    }
