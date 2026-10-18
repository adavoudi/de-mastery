"""MUTANT — too-weak streamhouse rule: only requires a stream-native store."""


def classify(signals: dict) -> str:
    if signals.get("two_parallel_pipelines"):
        return "lambda"
    # BUG: drops the "serves BOTH reads from one copy" requirement. A store that
    # serves only a streaming read (plain Fluss/Paimon as a stream buffer, no
    # OLAP read path) is NOT yet a streamhouse, but this calls it one.
    if signals.get("stream_native_store"):
        return "streamhouse"
    if signals.get("transactional_lake_table"):
        return "lakehouse"
    if signals.get("log_is_system_of_record"):
        return "kappa"
    raise ValueError(f"unclassifiable: {signals!r}")
