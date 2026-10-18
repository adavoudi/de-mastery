"""STH-01/01 — alt: a data-driven priority table instead of if/elif chain.

Each rule is (label, predicate); the first rule whose predicate holds wins.
Genuinely different structure from the reference branch chain, same semantics.
"""


_RULES = [
    ("lambda", lambda s: s.get("two_parallel_pipelines")),
    (
        "streamhouse",
        lambda s: s.get("stream_native_store")
        and s.get("serves_streaming_read")
        and s.get("serves_batch_read"),
    ),
    ("lakehouse", lambda s: s.get("transactional_lake_table")),
    ("kappa", lambda s: s.get("log_is_system_of_record")),
]


def classify(signals: dict) -> str:
    for label, predicate in _RULES:
        if predicate(signals):
            return label
    raise ValueError(f"unclassifiable architecture shape: {signals!r}")
