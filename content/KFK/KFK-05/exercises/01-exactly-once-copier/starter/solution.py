"""Starter — not implemented yet (must FAIL the tests: gate G1). See README.md."""


def copy_exactly_once(bootstrap_servers, src_topic, dst_topic, group_id,
                      transactional_id, max_records):
    # TODO: transactional read-process-write; commit output + input offsets atomically.
    return 0


def copy_then_abort(bootstrap_servers, src_topic, dst_topic, group_id,
                    transactional_id, max_records):
    # TODO: same as copy_exactly_once but abort_transaction() at the end.
    return 0
