"""Import the learner's submission module from ``DE_SUBMISSION_DIR``.

The directory is set by ``de test`` / ``de validate``. We import the ``entry`` file
(default ``solution.py``) as a *fresh* module every call so there is no caching of
state between validation runs over different candidate directories.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import uuid
from pathlib import Path
from types import ModuleType

SUBMISSION_ENV = "DE_SUBMISSION_DIR"
ENTRY_ENV = "DE_SUBMISSION_ENTRY"  # optional; defaults to solution.py


def import_submission(directory: str | None = None,
                      entry: str | None = None) -> ModuleType:
    directory = directory or os.environ.get(SUBMISSION_ENV)
    if not directory:
        raise RuntimeError(
            f"{SUBMISSION_ENV} is not set. The `submission` fixture is only usable "
            "when tests are launched by `de test` or `de validate`."
        )
    sub_dir = Path(directory)
    entry = entry or os.environ.get(ENTRY_ENV) or "solution.py"
    entry_path = sub_dir / entry
    if not entry_path.exists():
        raise FileNotFoundError(
            f"Submission entry {entry!r} not found in {sub_dir}. "
            f"Files present: {[p.name for p in sub_dir.iterdir()]}"
        )

    # Unique module name so repeated imports never collide/cache.
    mod_name = f"_de_submission_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(mod_name, entry_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not build an import spec for {entry_path}")
    module = importlib.util.module_from_spec(spec)

    # Make sibling files importable while loading (and restore sys.path after).
    added = str(sub_dir)
    inserted = added not in sys.path
    if inserted:
        sys.path.insert(0, added)
    try:
        sys.modules[mod_name] = module
        spec.loader.exec_module(module)
    finally:
        if inserted:
            try:
                sys.path.remove(added)
            except ValueError:
                pass
    return module
