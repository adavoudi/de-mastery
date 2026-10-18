"""Helpers for `de start` / `de test`: workspace management and running tests."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from de_cli.content import Exercise
from de_cli.java_support import spark_env
from de_cli.paths import workspace_root


def workspace_dir_for(ex: Exercise, root: Path | None = None) -> Path:
    """``workspace/<MODULE_ID>/<exercise dir name>``."""
    return workspace_root(root) / ex.module_id / ex.dir.name


def copy_starter(ex: Exercise, root: Path | None = None,
                 overwrite: bool = False) -> Path:
    dest = workspace_dir_for(ex, root)
    if dest.exists() and not overwrite:
        return dest
    if dest.exists():
        shutil.rmtree(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(ex.dir / "starter", dest)
    return dest


def entry_file(ex: Exercise) -> str:
    entry = ex.entry
    return entry[0] if isinstance(entry, list) else entry


def run_tests(ex: Exercise, submission_dir: Path, root: Path | None = None,
              capture: bool = False) -> int:
    """Run the exercise's tests against ``submission_dir``. Returns pytest exit code."""
    tests_dir = ex.dir / "tests"
    env = spark_env(os.environ)
    env["DE_SUBMISSION_DIR"] = str(submission_dir)
    env["DE_SUBMISSION_ENTRY"] = entry_file(ex)
    args = [
        sys.executable, "-m", "pytest", str(tests_dir),
        "-q", "-p", "no:cacheprovider",
        "-p", "no:randomly", f"--timeout={ex.timeout_s}",
    ]
    if capture:
        proc = subprocess.run(args, env=env, capture_output=True, text=True)
        return proc.returncode
    proc = subprocess.run(args, env=env)
    return proc.returncode
