"""Tests for LKH-07 / 01-decision-memo.

A machine-checkable rubric, not prose grading. The learner edits decision.yaml;
this test reads it from DE_SUBMISSION_DIR and checks (a) the single defensible
format per scenario and (b) that all required justification factors are present.

The rule table below is the oracle; each rule traces to a doc cited in lesson.md
(Delta=Databricks/Unity/liquid clustering; Iceberg=engine-neutral + partition
evolution; Hudi=record-level index + incremental pulls).
"""

import os
from pathlib import Path

import yaml

VALID_FORMATS = {"iceberg", "delta", "hudi"}

VOCAB = {
    "databricks_native", "unity_catalog", "liquid_clustering", "engine_neutral",
    "partition_evolution", "hidden_partitioning", "record_level_index",
    "upsert_heavy", "incremental_pulls", "append_only", "single_vendor",
    "multi_engine",
}

# (expected_format, required_all, required_any_groups)
RULES = {
    "scenario_1": ("delta", {"databricks_native"}, [{"unity_catalog", "liquid_clustering"}]),
    "scenario_2": ("iceberg", {"engine_neutral", "partition_evolution"}, []),
    "scenario_3": ("hudi", {"record_level_index", "incremental_pulls"}, []),
}


def _load():
    path = Path(os.environ["DE_SUBMISSION_DIR"]) / "decision.yaml"
    with open(path) as f:
        return yaml.safe_load(f)


def _factors(scn):
    raw = scn.get("factors") or []
    assert isinstance(raw, list), "factors must be a list"
    return set(raw)


def test_structure_valid():
    doc = _load()
    assert isinstance(doc, dict), "decision.yaml must be a mapping"
    for key in RULES:
        assert key in doc, f"missing {key}"
        assert "format" in doc[key], f"{key} missing 'format'"
        assert "factors" in doc[key], f"{key} missing 'factors'"


def test_formats_are_in_vocabulary():
    doc = _load()
    for key in RULES:
        fmt = doc[key]["format"]
        assert fmt in VALID_FORMATS, f"{key}: format {fmt!r} not one of {VALID_FORMATS}"


def test_factors_are_in_vocabulary():
    doc = _load()
    for key in RULES:
        extra = _factors(doc[key]) - VOCAB
        assert not extra, f"{key}: unknown factor keys {extra}"


def test_scenario_1_delta():
    doc = _load()
    _check(doc, "scenario_1")


def test_scenario_2_iceberg():
    doc = _load()
    _check(doc, "scenario_2")


def test_scenario_3_hudi():
    doc = _load()
    _check(doc, "scenario_3")


def _check(doc, key):
    expected_fmt, required_all, any_groups = RULES[key]
    scn = doc[key]
    assert scn["format"] == expected_fmt, (
        f"{key}: expected format {expected_fmt!r}, got {scn['format']!r}"
    )
    factors = _factors(scn)
    missing = required_all - factors
    assert not missing, f"{key}: missing required factors {missing}"
    for group in any_groups:
        assert factors & group, f"{key}: need at least one of {group}"
