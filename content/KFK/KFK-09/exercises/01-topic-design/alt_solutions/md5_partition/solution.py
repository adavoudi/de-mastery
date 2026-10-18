"""Alt 2: key by order_id (like the reference) but compute the partition with a
different stable hash (md5) and a different fold. Same guarantees."""

import hashlib


def message_key(event):
    return str(event["order_id"])


def partition_for(event, num_partitions):
    h = hashlib.md5(message_key(event).encode("utf-8")).hexdigest()
    return int(h, 16) % num_partitions


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
                "tenant_isolation",
            ],
        },
    }
