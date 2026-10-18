"""MUTANT — Lambda precedence dropped: log-as-record checked before Lambda."""


def classify(signals: dict) -> str:
    # BUG: a Lambda architecture can also replay a log in its speed layer, but
    # its defining shape is the two parallel pipelines. Checking kappa first
    # mislabels a Lambda (two pipelines + log) as kappa.
    if signals.get("log_is_system_of_record"):
        return "kappa"
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
    raise ValueError(f"unclassifiable: {signals!r}")
