"""Parse ``tracks/*.md`` syllabi to extract per-module hours.

Each syllabus has a markdown table whose first columns are ``ID | Module | h | ...``.
We read the ``ID`` column and the ``h`` (hours) column. This is the fallback source
of hours for modules that have not been built yet (no ``module.yaml``).
"""

from __future__ import annotations

import re
from pathlib import Path

from de_cli.paths import tracks_root

# Module IDs look like FND-01, SPK-12, KFK-03.
_ID_RE = re.compile(r"^[A-Z]{2,4}-\d{2}$")


def _parse_table(text: str) -> dict[str, float]:
    """Return {module_id: hours} from the first table that has ID and h columns."""
    hours: dict[str, float] = {}
    header_cols: list[str] | None = None
    id_idx = h_idx = None
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            header_cols = None
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        # Separator row like |---|---|
        if all(set(c) <= set("-: ") and c for c in cells):
            continue
        if header_cols is None:
            header_cols = [c.lower() for c in cells]
            try:
                id_idx = header_cols.index("id")
                h_idx = header_cols.index("h")
            except ValueError:
                header_cols = None
                id_idx = h_idx = None
            continue
        # Data row
        if id_idx is None or h_idx is None:
            continue
        if id_idx >= len(cells) or h_idx >= len(cells):
            continue
        mod_id = cells[id_idx]
        if not _ID_RE.match(mod_id):
            continue
        hv = cells[h_idx]
        m = re.search(r"\d+(?:\.\d+)?", hv)
        if m:
            hours[mod_id] = float(m.group())
    return hours


def track_hours(root: Path | None = None) -> dict[str, float]:
    """Return {module_id: hours} across every ``tracks/*.md`` syllabus."""
    out: dict[str, float] = {}
    tdir = tracks_root(root)
    if not tdir.is_dir():
        return out
    for md in sorted(tdir.glob("*.md")):
        out.update(_parse_table(md.read_text()))
    return out
