"""Scheduling math for `de plan`."""

from datetime import date, timedelta

from de_cli.plan import compute_plan


def _write_repo(root, learner_yaml: str, tracks: dict[str, int]):
    (root / "pyproject.toml").write_text("[project]\nname='t'\nversion='0'\n")
    (root / "learner.yaml").write_text(learner_yaml)
    tdir = root / "tracks"
    tdir.mkdir()
    # Build a syllabus table per track prefix with the 'h' column.
    by_prefix: dict[str, list[tuple[str, int]]] = {}
    for mid, h in tracks.items():
        by_prefix.setdefault(mid.split("-")[0], []).append((mid, h))
    for prefix, rows in by_prefix.items():
        lines = [f"# {prefix}", "", "| ID | Module | h | Key |",
                 "|---|---|---|---|"]
        for mid, h in rows:
            lines.append(f"| {mid} | Some module | {h} | ideas |")
        (tdir / f"{prefix}.md").write_text("\n".join(lines) + "\n")


def test_no_target_date_projects_finish(tmp_path):
    _write_repo(tmp_path,
                "pace:\n  hours_per_week: 10\n  target_date:\n",
                {"FND-01": 20, "SPK-01": 20})
    res = compute_plan(root=tmp_path, today=date(2026, 1, 1))
    assert res.target_date is None
    assert res.total_hours == 40
    assert res.remaining_hours == 40
    # 40h / 10 per week = 4 weeks
    assert res.projected_finish == date(2026, 1, 1) + timedelta(weeks=4)


def test_target_date_feasible(tmp_path):
    # 40h, 10 weeks away, pace 10/wk -> need 4/wk -> feasible
    target = date(2026, 1, 1) + timedelta(weeks=10)
    _write_repo(tmp_path,
                f"pace:\n  hours_per_week: 10\n  target_date: {target.isoformat()}\n",
                {"FND-01": 20, "SPK-01": 20})
    res = compute_plan(root=tmp_path, today=date(2026, 1, 1))
    assert res.feasible is True
    assert abs(res.required_per_week - 4.0) < 1e-6


def test_target_date_infeasible(tmp_path):
    # 100h, 2 weeks away, pace 6/wk -> need 50/wk -> not feasible
    target = date(2026, 1, 1) + timedelta(weeks=2)
    _write_repo(tmp_path,
                f"pace:\n  hours_per_week: 6\n  target_date: {target.isoformat()}\n",
                {"FND-01": 50, "SPK-01": 50})
    res = compute_plan(root=tmp_path, today=date(2026, 1, 1))
    assert res.feasible is False
    assert res.required_per_week > 6
    assert "Not feasible" in res.message


def test_skip_modules_reduces_hours(tmp_path):
    _write_repo(tmp_path,
                "pace:\n  hours_per_week: 10\n  target_date:\n"
                "tracks:\n  skip_modules: [SPK-01]\n",
                {"FND-01": 20, "SPK-01": 20})
    res = compute_plan(root=tmp_path, today=date(2026, 1, 1))
    assert res.total_hours == 20
