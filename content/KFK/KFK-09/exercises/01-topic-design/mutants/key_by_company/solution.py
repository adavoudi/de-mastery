"""Mutant: key by company_id -> a skewed big tenant hot-spots one partition."""

import hashlib


def message_key(event):
    # BUG: low-cardinality tenant key -> all of a big tenant's orders collide.
    return str(event["company_id"])


def partition_for(event, num_partitions):
    key = message_key(event).encode("utf-8")
    h = int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "big")
    return h % num_partitions


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
