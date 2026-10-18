"""BYO (build-your-own) challenge discovery and model.

Layout (see docs/PLATFORM.md)::

    byo/<name>/
      byo.yaml                 # id (e.g. SPK-BYO), title, track, hours, stages[]
      starter/                 # ONE cumulative learner codebase (copied on start)
      stage-01-<slug>/
        README.md              # the task for this stage
        stage.yaml             # hints (x3), timeout_s
        tests/                 # pytest files for THIS stage
        solution/              # cumulative reference that passes stages 1..N
        alt_solutions/<name>/  # >=1 genuinely different cumulative solution
        mutants/<name>/        # >=3 wrong cumulative solutions (each + WHY.md)
      stage-02-<slug>/ ...

Key rule: a stage's reference/alt/mutant directory is a *cumulative* codebase (it
contains the whole implementation up to and including that stage). Validation runs
the tests of every stage ``<= N`` against each candidate, so earlier stages are a
regression suite (docs/TESTING.md "BYO challenge tests").

IDs:  challenge id ``SPK-BYO``; a stage is addressed as ``SPK-BYO/03`` (the stage
NUMBER, zero-padded or not) or ``SPK-BYO/03-<slug>``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from de_cli.paths import repo_root


def byo_root(root: Path | None = None) -> Path:
    return (root or repo_root()) / "byo"


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text()) or {}


@dataclass
class Stage:
    challenge_id: str          # e.g. SPK-BYO
    number: int                # 1-based
    slug: str                  # e.g. scan-parquet
    dir: Path
    meta: dict

    @property
    def id(self) -> str:
        return f"{self.challenge_id}/{self.number:02d}-{self.slug}"

    @property
    def short_id(self) -> str:
        return f"{self.challenge_id}/{self.number:02d}"

    @property
    def title(self) -> str:
        return self.meta.get("title", self.slug.replace("-", " "))

    @property
    def timeout_s(self) -> int:
        return int(self.meta.get("timeout_s", 300))

    @property
    def hints(self) -> list[str]:
        return list(self.meta.get("hints", []))


@dataclass
class Challenge:
    id: str                    # e.g. SPK-BYO
    dir: Path
    meta: dict
    stages: list[Stage] = field(default_factory=list)

    @property
    def track(self) -> str:
        return self.meta.get("track") or self.id.split("-")[0]

    @property
    def title(self) -> str:
        return self.meta.get("title", self.id)

    @property
    def hours(self) -> float:
        return float(self.meta.get("hours", 0) or 0)

    @property
    def starter_dir(self) -> Path:
        return self.dir / "starter"

    def stage_by(self, token: str) -> Stage | None:
        """Resolve a stage from ``NN`` or ``NN-slug`` (number is authoritative)."""
        m = re.match(r"0*(\d+)", token)
        if not m:
            return None
        num = int(m.group(1))
        for s in self.stages:
            if s.number == num:
                return s
        return None


_STAGE_RE = re.compile(r"^stage-(\d+)-(.+)$")


def load_challenge(challenge_dir: Path) -> Challenge:
    meta = _read_yaml(challenge_dir / "byo.yaml")
    cid = meta.get("id", challenge_dir.name)
    ch = Challenge(id=cid, dir=challenge_dir, meta=meta)
    # Stage order: byo.yaml `stages:` list if given, else sorted dir names.
    declared = meta.get("stages")
    stage_dirs = {}
    for p in challenge_dir.iterdir():
        if p.is_dir():
            m = _STAGE_RE.match(p.name)
            if m:
                stage_dirs[p.name] = (int(m.group(1)), m.group(2), p)
    if declared:
        order = []
        for i, slug in enumerate(declared, start=1):
            name = f"stage-{i:02d}-{slug}"
            if name in stage_dirs:
                order.append(stage_dirs[name])
    else:
        order = sorted(stage_dirs.values(), key=lambda t: t[0])
    for num, slug, d in order:
        ch.stages.append(
            Stage(challenge_id=cid, number=num, slug=slug, dir=d,
                  meta=_read_yaml(d / "stage.yaml"))
        )
    return ch


def discover_challenges(root: Path | None = None) -> list[Challenge]:
    """Find all ``byo/*/byo.yaml`` under ``<root>/byo`` (or a direct byo dir).

    ``root`` may also point straight at a ``byo`` directory or a single challenge
    directory (containing ``byo.yaml``), for ``--byo-root`` targeting.
    """
    base = byo_root(root) if root is None else Path(root)
    if (base / "byo.yaml").exists():            # a single challenge dir
        return [load_challenge(base)]
    if base.name != "byo" and (base / "byo").is_dir():
        base = base / "byo"
    out: list[Challenge] = []
    if not base.is_dir():
        return out
    for y in sorted(base.glob("*/byo.yaml")):
        out.append(load_challenge(y.parent))
    return out


def find_challenge(challenge_id: str, root: Path | None = None) -> Challenge | None:
    for c in discover_challenges(root):
        if c.id == challenge_id:
            return c
    return None


def find_stage(stage_id: str, root: Path | None = None) -> tuple[Challenge, Stage] | None:
    """Resolve ``SPK-BYO/03`` or ``SPK-BYO/03-slug`` to (challenge, stage)."""
    if "/" not in stage_id:
        return None
    cid, token = stage_id.split("/", 1)
    ch = find_challenge(cid, root)
    if not ch:
        return None
    st = ch.stage_by(token)
    if not st:
        return None
    return ch, st


def all_stages(root: Path | None = None) -> list[Stage]:
    out: list[Stage] = []
    for c in discover_challenges(root):
        out.extend(c.stages)
    return out
