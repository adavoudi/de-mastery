"""MUTANT — wrong precedence: lake table checked BEFORE the streamhouse rule."""


def classify(signals: dict) -> str:
    if signals.get("two_parallel_pipelines"):
        return "lambda"
    # BUG: a streamhouse also has a lake table underneath, so checking the
    # lakehouse rule first mislabels every streamhouse as a lakehouse.
    if signals.get("transactional_lake_table"):
        return "lakehouse"
    if (
        signals.get("stream_native_store")
        and signals.get("serves_streaming_read")
        and signals.get("serves_batch_read")
    ):
        return "streamhouse"
    if signals.get("log_is_system_of_record"):
        return "kappa"
    raise ValueError(f"unclassifiable: {signals!r}")
