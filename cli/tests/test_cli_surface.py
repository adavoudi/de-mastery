"""Smoke tests for the `de` command surface (guards against Typer/Click incompatibilities)."""

import subprocess
import sys

import pytest

COMMANDS = ["status", "next", "start", "test", "hint", "solution", "validate", "plan",
            "quiz", "doctor", "lab", "mock", "review", "cost", "dispute"]


def _de(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-m", "de_cli.main", *args],
                          capture_output=True, text=True, timeout=60)


def test_root_help_lists_all_commands():
    out = _de("--help")
    assert out.returncode == 0, out.stderr
    for cmd in COMMANDS:
        assert cmd in out.stdout, f"{cmd} missing from --help"


@pytest.mark.parametrize("cmd", COMMANDS)
def test_each_command_help_renders(cmd):
    out = _de(cmd, "--help")
    assert out.returncode == 0, out.stderr[-2000:]
    assert "Traceback" not in out.stdout + out.stderr
