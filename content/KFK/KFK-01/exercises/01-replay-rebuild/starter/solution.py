"""Starter — not implemented yet (must FAIL the tests: gate G1).

Rebuild a 'latest value per key' table by replaying a topic from a given offset
or from a timestamp. See README.md.
"""


def rebuild_from_offset(bootstrap_servers, topic, start_offsets):
    # TODO: assign each partition at its start offset, fold records (last wins)
    #       up to the high watermark, and return {account_id: latest_balance}.
    return {}


def rebuild_from_timestamp(bootstrap_servers, topic, since_ms):
    # TODO: use offsets_for_times to find the first offset >= since_ms per
    #       partition, then fold forward like rebuild_from_offset.
    return {}
