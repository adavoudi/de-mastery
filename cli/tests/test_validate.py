"""validate gates (G1-G5) on the fixture exercises."""

from de_cli.content import find_exercise
from de_cli.validate import validate_exercise


def test_dedup_passes_all_gates(fixture_content_root):
    ex = find_exercise("ZZZ-01/01-dedup-orders", fixture_content_root)
    assert ex is not None
    result = validate_exercise(ex, runs=3)
    failed = [g for g in result.gates if not g.passed]
    assert result.ok, f"expected all gates to pass, failed: {[(g.gate, g.detail) for g in failed]}"
    # Every documented gate should be present.
    gate_ids = {g.gate for g in result.gates}
    assert {"G1", "G2", "G3", "G4", "G5"} <= gate_ids


def test_broken_exercise_fails_g1(fixture_content_root):
    ex = find_exercise("ZZZ-01/02-broken", fixture_content_root)
    assert ex is not None
    result = validate_exercise(ex, runs=2)
    assert not result.ok, "broken exercise should fail validation"
    g1 = next(g for g in result.gates if g.gate == "G1")
    assert not g1.passed, "G1 (starter must fail) should be the failing gate"


def test_individual_mutants_are_killed(fixture_content_root):
    """Each mutant, run directly, must fail at least one test (exit nonzero)."""
    from de_cli.validate import _run_pytest
    ex = find_exercise("ZZZ-01/01-dedup-orders", fixture_content_root)
    mut_root = ex.dir / "mutants"
    mutants = [p for p in mut_root.iterdir() if p.is_dir()]
    assert len(mutants) >= 3
    for m in mutants:
        rc = _run_pytest(ex.dir / "tests", m, "solution.py", ex.timeout_s)
        assert rc != 0, f"mutant survived (should fail): {m.name}"
