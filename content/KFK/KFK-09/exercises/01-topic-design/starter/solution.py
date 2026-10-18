"""Starter — not implemented yet (must FAIL the tests: gate G1)."""


def message_key(event):
    # TODO: return a key that co-locates all events of ONE order while spreading
    # a skewed tenant's orders across partitions.
    return event["company_id"]


def partition_for(event, num_partitions):
    # TODO: a stable, deterministic hash of message_key(event) % num_partitions.
    return 0


def design():
    # TODO: fill in the orders/customers design per the README rubric.
    return {}
