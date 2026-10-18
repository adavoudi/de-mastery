"""Discover and load content modules and exercises from a content root."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from de_cli.paths import content_root


@dataclass
class Exercise:
    id: str                     # <MODULE_ID>/<dir name>
    dir: Path
    meta: dict
    module_id: str

    @property
    def entry(self) -> str | list[str]:
        return self.meta.get("entry", "solution.py")

    @property
    def timeout_s(self) -> int:
        return int(self.meta.get("timeout_s", 300))

    @property
    def hints(self) -> list[str]:
        return list(self.meta.get("hints", []))


@dataclass
class Module:
    id: str
    dir: Path
    meta: dict
    exercises: list[Exercise] = field(default_factory=list)

    @property
    def track(self) -> str:
        return self.meta.get("track") or self.id.split("-")[0]

    @property
    def hours(self) -> float:
        return float(self.meta.get("hours", 0) or 0)

    @property
    def requires(self) -> list[str]:
        return list(self.meta.get("requires", []))


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text())
    return data or {}


def load_module(module_dir: Path) -> Module:
    meta = _read_yaml(module_dir / "module.yaml")
    mod_id = meta.get("id", module_dir.name)
    module = Module(id=mod_id, dir=module_dir, meta=meta)
    ex_root = module_dir / "exercises"
    if ex_root.is_dir():
        for ex_dir in sorted(p for p in ex_root.iterdir() if p.is_dir()):
            ex_meta = _read_yaml(ex_dir / "exercise.yaml")
            ex_id = ex_meta.get("id", f"{mod_id}/{ex_dir.name}")
            module.exercises.append(
                Exercise(id=ex_id, dir=ex_dir, meta=ex_meta, module_id=mod_id)
            )
    return module


def discover_modules(content_dir: Path | None = None) -> list[Module]:
    """Find all modules under ``<content_dir>/<TRACK>/<MODULE_ID>/module.yaml``.

    ``content_dir`` defaults to ``<repo>/content``. If it points directly at a single
    module directory (contains ``module.yaml``), that one module is returned.
    """
    search = content_dir if content_dir is not None else content_root()
    search = Path(search)
    if (search / "module.yaml").exists():
        return [load_module(search)]
    modules: list[Module] = []
    if not search.is_dir():
        return modules
    for mod_yaml in sorted(search.glob("*/*/module.yaml")):
        modules.append(load_module(mod_yaml.parent))
    return modules


def find_module(module_id: str, content_dir: Path | None = None) -> Module | None:
    for m in discover_modules(content_dir):
        if m.id == module_id:
            return m
    return None


def find_exercise(exercise_id: str, content_dir: Path | None = None) -> Exercise | None:
    for m in discover_modules(content_dir):
        for ex in m.exercises:
            if ex.id == exercise_id:
                return ex
    return None


def all_exercises(content_dir: Path | None = None) -> list[Exercise]:
    out: list[Exercise] = []
    for m in discover_modules(content_dir):
        out.extend(m.exercises)
    return out
