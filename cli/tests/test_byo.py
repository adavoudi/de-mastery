"""BYO challenge discovery, stage resolution, and validity gates G1-G5."""

from pathlib import Path

import pytest

from de_cli import byo as byo_mod
from de_cli import byo_runner as byo_runner_mod

FIXTURE_BYO = Path(__file__).parent / "fixtures" / "byo"


@pytest.fixture
def challenge():
    chs = byo_mod.discover_challenges(FIXTURE_BYO)
    assert len(chs) == 1
    return chs[0]


def test_discovery_and_ids(challenge):
    assert challenge.id == "ZZZ-BYO"
    assert challenge.track == "ZZZ"
    assert challenge.hours == 2
    assert [s.number for s in challenge.stages] == [1, 2]
    assert challenge.stages[0].id == "ZZZ-BYO/01-scan"
    assert challenge.stages[1].short_id == "ZZZ-BYO/02"


def test_find_stage_by_number_and_slug():
    f1 = byo_mod.find_stage("ZZZ-BYO/01", FIXTURE_BYO)
    f2 = byo_mod.find_stage("ZZZ-BYO/01-scan", FIXTURE_BYO)
    assert f1 is not None and f2 is not None
    assert f1[1].id == f2[1].id == "ZZZ-BYO/01-scan"
    assert byo_mod.find_stage("ZZZ-BYO/99", FIXTURE_BYO) is None


def test_stage1_all_gates_pass(challenge):
    st = challenge.stages[0]
    result = byo_runner_mod.validate_stage(challenge, st, runs=3)
    failed = [(g.gate, g.detail) for g in result.gates if not g.passed]
    assert result.ok, f"stage 1 gates failed: {failed}"
    assert {"G1", "G2", "G3", "G4", "G5"} == {g.gate for g in result.gates}


def test_stage2_all_gates_pass_with_regression(challenge):
    """Stage 2 validation runs stage 1 + stage 2 tests against each candidate."""
    st = challenge.stages[1]
    result = byo_runner_mod.validate_stage(challenge, st, runs=3)
    failed = [(g.gate, g.detail) for g in result.gates if not g.passed]
    assert result.ok, f"stage 2 gates failed: {failed}"


def test_stage2_mutants_fail_cumulative(challenge):
    """Each stage-2 mutant has a correct scan (passes stage 1) but a broken
    group_sum, so it must fail when stages 1..2 run together (G4)."""
    st = challenge.stages[1]
    mut_root = st.dir / "mutants"
    mutants = [p for p in mut_root.iterdir() if p.is_dir()]
    assert len(mutants) >= 3
    for m in mutants:
        rc = byo_runner_mod.run_stage(challenge, st, m)
        assert rc != 0, f"stage-2 mutant survived cumulative run: {m.name}"
        # but it should still pass stage 1 alone (proves regression isolation)
        rc1 = byo_runner_mod.run_stage(challenge, challenge.stages[0], m,
                                       only_this_stage=True)
        assert rc1 == 0, f"stage-2 mutant unexpectedly broke stage 1: {m.name}"


def test_starter_fails_stage1(challenge):
    """G1: the cumulative starter fails stage 1's own tests."""
    st = challenge.stages[0]
    rc = byo_runner_mod.run_stage(challenge, st, challenge.starter_dir,
                                  only_this_stage=True)
    assert rc != 0
