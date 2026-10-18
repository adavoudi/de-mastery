"""STH-01/01 — alt: compute ALL matching labels, then pick the highest-priority.

Instead of short-circuiting on the first match, build the set of labels whose
shape the signals satisfy, then return the one with the smallest priority rank.
Different control flow (collect-then-rank) from both other solutions.
"""

_PRIORITY = ["lambda", "streamhouse", "lakehouse", "kappa"]


def _matches(signals: dict) -> set:
    out = set()
    if signals.get("two_parallel_pipelines"):
        out.add("lambda")
    if (
        signals.get("stream_native_store")
        and signals.get("serves_streaming_read")
        and signals.get("serves_batch_read")
    ):
        out.add("streamhouse")
    if signals.get("transactional_lake_table"):
        out.add("lakehouse")
    if signals.get("log_is_system_of_record"):
        out.add("kappa")
    return out


def classify(signals: dict) -> str:
    candidates = _matches(signals)
    for label in _PRIORITY:
        if label in candidates:
            return label
    raise ValueError(f"unclassifiable architecture shape: {signals!r}")
