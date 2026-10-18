"""Mutant: correct partitioning, but the design() choices are wrong — orders are
declared compacted/entity and customers time/event (swapped), which loses order
history and makes customer state non-rebuildable."""

import hashlib


def message_key(event):
    return str(event["order_id"])


def partition_for(event, num_partitions):
    key = message_key(event).encode("utf-8")
    h = int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "big")
    return h % num_partitions


def design():
    return {
        "orders": {
            # BUG: orders should be a time-retained EVENT topic, not compacted.
            "topic_type": "entity",
            "retention": "compacted",
            "key": "order_id",
            "factors": [
                "latest_state_per_key",
                "log_compaction",
            ],
        },
        "customers": {
            # BUG: customers should be a COMPACTED entity topic, not a time event.
            "topic_type": "event",
            "retention": "time",
            "key": "customer_id",
            "factors": [
                "append_only_history",
                "time_retention",
            ],
        },
    }
