"""STH-01/01 — reference: classify by storage + pipeline shape (priority order)."""


def classify(signals: dict) -> str:
    """Return exactly one of 'lambda', 'kappa', 'lakehouse', 'streamhouse'.

    Signals are tested from most-specific to least-specific so a platform that
    sets several at once (a streamhouse also has a lake table underneath) is
    labelled by its defining shape.
    """
    if signals.get("two_parallel_pipelines"):
        return "lambda"
    if (
        signals.get("stream_native_store")
        and signals.get("serves_streaming_read")
        and signals.get("serves_batch_read")
    ):
        return "streamhouse"
    if signals.get("transactional_lake_table"):
        return "lakehouse"
    if signals.get("log_is_system_of_record"):
        return "kappa"
    raise ValueError(f"unclassifiable architecture shape: {signals!r}")
