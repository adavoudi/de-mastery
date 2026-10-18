"""Course library: modules, exercises, BYO challenges and docs, read from a checkout of the repo.

Pages are addressed by ids from the index, never by user-supplied paths, so a URL can only ever
reach a file the index chose to expose. Solutions, mutants and tests are never exposed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path, PurePosixPath

import yaml

from de_cli import byo as byo_mod
from de_web import render as render_mod

DEFAULT_TRACK_ORDER = ["FND", "SPK", "LKH", "JVM", "KFK", "FLK", "STH"]
MAX_STARTER_BYTES = 60_000


def _yaml(path: Path) -> dict:
    try:
        return yaml.safe_load(path.read_text()) or {}
    except (OSError, yaml.YAMLError):
        return {}


def _read(path: Path) -> str:
    try:
        return path.read_text()
    except (OSError, UnicodeDecodeError):
        return ""


@dataclass
class Exercise:
    id: str
    slug: str
    title: str
    dir: Path
    module_id: str
    hints: list[str]
    meta: dict

    @property
    def readme(self) -> Path:
        return self.dir / "README.md"

    def starter_files(self) -> list[tuple[str, str]]:
        out = []
        sdir = self.dir / "starter"
        if sdir.is_dir():
            for p in sorted(sdir.rglob("*")):
                if p.is_file() and "__pycache__" not in p.parts and p.stat().st_size <= MAX_STARTER_BYTES:
                    text = _read(p)
                    if text:
                        out.append((p.relative_to(sdir).as_posix(), text))
        return out


@dataclass
class Module:
    id: str
    track: str
    title: str
    dir: Path
    meta: dict
    exercises: list[Exercise] = field(default_factory=list)

    @property
    def hours(self) -> float:
        return float(self.meta.get("hours") or 0)

    def has(self, name: str) -> bool:
        return (self.dir / name).is_file()


@dataclass
class Track:
    id: str
    name: str
    hours: str
    syllabus: Path | None
    modules: list[Module] = field(default_factory=list)
    blurb: str = ""


@dataclass
class Stage:
    id: str
    slug: str
    number: int
    title: str
    dir: Path
    hints: list[str]


@dataclass
class Challenge:
    id: str
    title: str
    track: str
    hours: float
    stages: list[Stage]
    starter_dir: Path


@dataclass
class Doc:
    slug: str
    title: str
    path: Path
    group: str


class Library:
    def __init__(self, root: Path, source_url: str = "https://github.com/adavoudi/de-mastery"):
        self.root = root
        self.source_url = source_url.rstrip("/")
        self.tracks: dict[str, Track] = {}
        self.modules: dict[str, Module] = {}
        self.exercises: dict[str, Exercise] = {}
        self.challenges: dict[str, Challenge] = {}
        self.docs: dict[str, Doc] = {}
        self._routes: dict[str, str] = {}
        self._load()

    # -- loading --------------------------------------------------------------
    def _load(self) -> None:
        order = DEFAULT_TRACK_ORDER
        for name in ("learner.yaml", "learner.example.yaml"):
            cfg = _yaml(self.root / name)
            if (cfg.get("tracks") or {}).get("order"):
                order = cfg["tracks"]["order"]
                break

        for md in sorted((self.root / "tracks").glob("*.md")):
            tid = md.name.split("-")[0]
            head = _read(md).splitlines()[0] if md.exists() else ""
            m = re.match(r"#\s*([A-Z]+):\s*(.+?)(?:\s*\((.+)\))?\s*$", head)
            name, hours = (m.group(2), m.group(3) or "") if m else (tid, "")
            self.tracks[tid] = Track(tid, name, hours, md)
            self._routes[f"tracks/{md.name}"] = f"/track/{tid}"

        for mod_yaml in sorted((self.root / "content").glob("*/*/module.yaml")):
            meta = _yaml(mod_yaml)
            mdir = mod_yaml.parent
            mid = meta.get("id", mdir.name)
            lesson_title = (_read(mdir / "lesson.md").splitlines() or [""])[0]
            title = meta.get("title") or re.sub(r"^#\s*\S+\s*·\s*", "", lesson_title) or mid
            mod = Module(mid, meta.get("track") or mid.split("-")[0], title, mdir, meta)
            ex_root = mdir / "exercises"
            if ex_root.is_dir():
                for ed in sorted(p for p in ex_root.iterdir() if p.is_dir()):
                    em = _yaml(ed / "exercise.yaml")
                    ex = Exercise(em.get("id", f"{mid}/{ed.name}"), ed.name,
                                  em.get("title", ed.name.replace("-", " ")), ed, mid,
                                  list(em.get("hints", [])), em)
                    mod.exercises.append(ex)
                    self.exercises[ex.id] = ex
                    self._routes[f"content/{mdir.parent.name}/{mdir.name}/exercises/{ed.name}/README.md"] = \
                        f"/m/{mid}/x/{ed.name}"
            self.modules[mid] = mod
            base = f"content/{mdir.parent.name}/{mdir.name}"
            self._routes[f"{base}/lesson.md"] = f"/m/{mid}"
            for part in ("drills", "explain"):
                self._routes[f"{base}/{part}.md"] = f"/m/{mid}/{part}"
            self._routes[f"{base}/quiz.yaml"] = f"/m/{mid}/quiz"
            if mod.track not in self.tracks:
                self.tracks[mod.track] = Track(mod.track, mod.track, "", None)
            self.tracks[mod.track].modules.append(mod)

        # syllabus-only tracks (no published modules) are not part of the site
        self.tracks = {k: t for k, t in self.tracks.items() if t.modules}
        # order tracks: configured order first, then any others
        rank = {t: i for i, t in enumerate(order)}
        self.tracks = dict(sorted(self.tracks.items(), key=lambda kv: (rank.get(kv[0], 99), kv[0])))
        for t in self.tracks.values():
            t.modules.sort(key=lambda m: m.id)
            t.blurb = self._blurb(t)

        for ch in byo_mod.discover_challenges(self.root / "byo"):
            stages = [Stage(s.id, s.slug, s.number, s.title, s.dir, s.hints) for s in ch.stages]
            self.challenges[ch.id] = Challenge(ch.id, ch.title, ch.track, ch.hours, stages, ch.starter_dir)
            for s in stages:
                self._routes[f"byo/{ch.dir.name}/{s.dir.name}/README.md"] = f"/byo/{ch.id}/{s.number:02d}"

        for slug, rel, title, group in [
            ("tutorial", "docs/TUTORIAL.md", "How to study with this course", "Guide"),
            ("platform", "docs/PLATFORM.md", "Platform design", "Guide"),
            ("testing", "docs/TESTING.md", "How the tests are proven valid", "Guide"),
            ("web", "docs/WEB.md", "Running the course website", "Guide"),
        ]:
            if (self.root / rel).is_file():
                self.docs[slug] = Doc(slug, title, self.root / rel, group)
                self._routes[rel] = f"/docs/{slug}"
        self._routes["ROADMAP.md"] = "/roadmap"
        self._routes["README.md"] = "/"

    @staticmethod
    def _blurb(track: Track) -> str:
        if not track.syllabus:
            return ""
        body = _read(track.syllabus)
        m = re.search(r"\*\*(?:Philosophy|Why)\.\*\*\s*(.+?)(?:\n\n|\Z)", body, re.S)
        if m:
            text = m.group(1)
        else:  # first plain paragraph (not a heading, table, list or quote)
            paras = [p.strip() for p in re.split(r"\n\s*\n", body)]
            plain = [p for p in paras if p and not re.match(r"[#|>\-*\d`]", p)]
            if not plain:
                return ""
            text = plain[0]
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"\*\*|`", "", text)
        first = re.split(r"(?<=[.!?])\s", text, maxsplit=1)[0]
        return first

    # -- navigation -----------------------------------------------------------
    def ordered_modules(self) -> list[Module]:
        return [m for t in self.tracks.values() for m in t.modules]

    def neighbours(self, module_id: str) -> tuple[Module | None, Module | None]:
        mods = self.ordered_modules()
        for i, m in enumerate(mods):
            if m.id == module_id:
                return (mods[i - 1] if i else None), (mods[i + 1] if i + 1 < len(mods) else None)
        return None, None

    def all_exercise_ids(self) -> list[str]:
        ids = [e.id for m in self.ordered_modules() for e in m.exercises]
        ids += [s.id for c in self.challenges.values() for s in c.stages]
        return ids

    # -- content --------------------------------------------------------------
    def make_resolver(self, source_rel: str):
        """Link resolver for a markdown file at repo-relative ``source_rel``."""
        base = PurePosixPath(source_rel)

        def resolve(href: str) -> tuple[str, bool]:
            if re.match(r"^[a-z][a-z0-9+.-]*:", href, re.I) or href.startswith("//"):
                return href, True
            if href.startswith("#") or not href:
                return href, False
            rel, frag = render_mod.normalize_href(base, href)
            suffix = f"#{frag}" if frag else ""
            if rel in self._routes:
                return self._routes[rel] + suffix, False
            return f"{self.source_url}/blob/main/{rel}{suffix}", True

        return resolve

    def rel(self, path: Path) -> str:
        return path.resolve().relative_to(self.root.resolve()).as_posix()

    def render_file(self, path: Path) -> render_mod.Rendered:
        return _render_cached(self, path)

    def render_text(self, text: str, source: Path) -> render_mod.Rendered:
        return render_mod.render(text, self.make_resolver(self.rel(source)), source.parent)


@lru_cache(maxsize=1024)
def _render_cached(lib: Library, path: Path) -> render_mod.Rendered:
    return render_mod.render(_read(path), lib.make_resolver(lib.rel(path)), path.parent)


def load_quiz(module: Module) -> list[dict]:
    path = module.dir / "quiz.yaml"
    if not path.is_file():
        return []
    try:
        data = yaml.safe_load(path.read_text()) or []
    except yaml.YAMLError:
        return []
    return data if isinstance(data, list) else data.get("questions", [])
