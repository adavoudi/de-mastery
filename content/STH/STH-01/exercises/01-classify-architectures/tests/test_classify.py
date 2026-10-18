"""STH-01/01 — classify 5 real architectures with a deterministic rubric.

The learner implements ``classify(signals) -> str``. We grade against an
INDEPENDENT oracle (a priority-scan written a different way than any shipped
solution), over five real-world architectures described from public engineering
write-ups as boolean signal dicts. Pure Python: no JVM, no Docker, always runs.
"""

import pytest

# ---------------------------------------------------------------------------
# Five real architectures, described by their STORAGE + PIPELINE SHAPE.
# (Signals, not vendor labels. Sources are public engineering descriptions.)
# ---------------------------------------------------------------------------
#  - Classic Lambda: a batch recompute layer + a speed layer merged at serving,
#    with Kafka replayed by the speed layer (two_parallel_pipelines AND a log).
#  - Classic Kappa: one streaming pipeline, the log is the system of record and
#    is replayed to reprocess history; no transactional lake table primary store.
#  - Batch lakehouse: one Iceberg/Delta table on object storage served to BI at
#    batch latency; Kafka is only an ingestion bus (NOT system of record).
#  - Streamhouse (Paimon/Fluss on the lake): one stream-native copy serves BOTH
#    a streaming/changelog read AND a batch/OLAP read.
#  - Stream buffer only: a stream-native store that currently serves ONLY a
#    streaming read (no OLAP read path from the same copy) -> still a lakehouse-
#    less kappa-shaped store; NOT yet a streamhouse.
ARCHITECTURES = {
    "classic_lambda": {
        "signals": {
            "two_parallel_pipelines": True,
            "log_is_system_of_record": True,
            "transactional_lake_table": True,
            "stream_native_store": False,
            "serves_streaming_read": True,
            "serves_batch_read": True,
        },
        "expected": "lambda",
    },
    "classic_kappa": {
        "signals": {
            "two_parallel_pipelines": False,
            "log_is_system_of_record": True,
            "transactional_lake_table": False,
            "stream_native_store": False,
            "serves_streaming_read": True,
            "serves_batch_read": False,
        },
        "expected": "kappa",
    },
    "batch_lakehouse": {
        "signals": {
            "two_parallel_pipelines": False,
            "log_is_system_of_record": False,
            "transactional_lake_table": True,
            "stream_native_store": False,
            "serves_streaming_read": False,
            "serves_batch_read": True,
        },
        "expected": "lakehouse",
    },
    "paimon_fluss_streamhouse": {
        "signals": {
            "two_parallel_pipelines": False,
            "log_is_system_of_record": False,
            "transactional_lake_table": True,
            "stream_native_store": True,
            "serves_streaming_read": True,
            "serves_batch_read": True,
        },
        "expected": "streamhouse",
    },
    "stream_buffer_only": {
        "signals": {
            "two_parallel_pipelines": False,
            "log_is_system_of_record": True,
            "transactional_lake_table": False,
            "stream_native_store": True,
            "serves_streaming_read": True,
            "serves_batch_read": False,
        },
        "expected": "kappa",
    },
}


def _oracle(signals: dict) -> str:
    """Independent reference: a flat priority scan (different structure from any
    shipped solution). First rule whose shape matches wins."""
    rules = (
        ("lambda", signals.get("two_parallel_pipelines")),
        (
            "streamhouse",
            signals.get("stream_native_store")
            and signals.get("serves_streaming_read")
            and signals.get("serves_batch_read"),
        ),
        ("lakehouse", signals.get("transactional_lake_table")),
        ("kappa", signals.get("log_is_system_of_record")),
    )
    matched = [label for label, ok in rules if ok]
    if not matched:
        raise ValueError("unclassifiable")
    return matched[0]


@pytest.mark.parametrize("name", sorted(ARCHITECTURES))
def test_classification_matches_expected(submission, name):
    arch = ARCHITECTURES[name]
    got = submission.classify(dict(arch["signals"]))
    assert got == arch["expected"], f"{name}: got {got!r}, want {arch['expected']!r}"


@pytest.mark.parametrize("name", sorted(ARCHITECTURES))
def test_classification_matches_independent_oracle(submission, name):
    signals = dict(ARCHITECTURES[name]["signals"])
    assert submission.classify(signals) == _oracle(signals)


def test_lambda_beats_everything(submission):
    # A platform that sets Lambda's signal AND every other signal is still Lambda:
    # the two parallel pipelines are the defining shape (kills kappa-before-lambda).
    signals = {
        "two_parallel_pipelines": True,
        "log_is_system_of_record": True,
        "transactional_lake_table": True,
        "stream_native_store": True,
        "serves_streaming_read": True,
        "serves_batch_read": True,
    }
    assert submission.classify(signals) == "lambda"


def test_streamhouse_requires_both_reads(submission):
    # stream-native store that serves ONLY a streaming read is NOT a streamhouse
    # (kills streamhouse-single-read). It falls through to the next matching rule.
    signals = {
        "two_parallel_pipelines": False,
        "log_is_system_of_record": True,
        "transactional_lake_table": False,
        "stream_native_store": True,
        "serves_streaming_read": True,
        "serves_batch_read": False,
    }
    assert submission.classify(signals) == "kappa"


def test_streamhouse_over_lakehouse_precedence(submission):
    # a streamhouse also has a lake table underneath; it must NOT be labelled a
    # lakehouse (kills lakehouse-before-streamhouse).
    signals = ARCHITECTURES["paimon_fluss_streamhouse"]["signals"]
    assert submission.classify(dict(signals)) == "streamhouse"


def test_determinism_repeated_calls(submission):
    # Same input -> same output across repeated calls (no hidden state / randomness).
    signals = dict(ARCHITECTURES["paimon_fluss_streamhouse"]["signals"])
    results = {submission.classify(dict(signals)) for _ in range(5)}
    assert results == {"streamhouse"}


def test_unclassifiable_raises(submission):
    signals = {
        "two_parallel_pipelines": False,
        "log_is_system_of_record": False,
        "transactional_lake_table": False,
        "stream_native_store": False,
        "serves_streaming_read": False,
        "serves_batch_read": False,
    }
    with pytest.raises(ValueError):
        submission.classify(signals)
