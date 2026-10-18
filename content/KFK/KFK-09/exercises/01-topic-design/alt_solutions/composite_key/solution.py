"""Alt 1: use a COMPOSITE key "company_id:order_id". Still high-cardinality
(unique per order) so per-entity ordering holds and a big tenant's orders spread
across partitions; the company prefix aids routing/debugging. Different hash
(sha256) for placement."""

import hashlib


def message_key(event):
    return f"{event['company_id']}:{event['order_id']}"


def partition_for(event, num_partitions):
    digest = hashlib.sha256(message_key(event).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % num_partitions


def design():
    return {
        "orders": {
            "topic_type": "event",
            "retention": "time",
            "key": "order_id",
            "factors": [
                "per_entity_ordering",
                "high_cardinality_key",
                "avoid_hot_partition",
                "time_retention",
                "tenant_isolation",
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
