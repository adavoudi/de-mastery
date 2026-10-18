"""Reference: key by the high-cardinality entity id (order_id) so each order's
events are totally ordered on one partition, while a skewed tenant's many orders
spread across partitions. Partition via a stable blake2b hash (not salted hash())."""

import hashlib


def message_key(event):
    # The entity id is the ordering + co-location contract. company_id is carried
    # elsewhere (header/field) for tenant filtering, NOT used for placement.
    return str(event["order_id"])


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
                "time_retention",
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
                "tenant_isolation",
            ],
        },
    }
