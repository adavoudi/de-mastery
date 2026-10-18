"""Starter — not implemented yet (must FAIL the tests: gate G1)."""


def base_schema():
    # TODO: return the version-1 Order schema exactly as in the README.
    return {}


def evolved_schema():
    # TODO: return a BACKWARD-compatible version-2 schema adding `currency`.
    return {}


def produce_and_read(bootstrap_servers, topic, orders):
    # TODO: produce each order serialized with the evolved schema, then read
    # every record back with BOTH the new and the old reader schema.
    return {"new_reader": [], "old_reader": []}
