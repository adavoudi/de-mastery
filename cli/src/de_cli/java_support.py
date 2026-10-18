"""Java resolution for the CLI, delegating to de_testkit.java_env.

Kept as a separate module so CLI code imports a stable name regardless of where the
resolution logic lives.
"""

from __future__ import annotations

from de_testkit import java_env


def resolve() -> dict:
    # Resolution may be cached; the CLI is short-lived so that's fine.
    java_env.resolve.cache_clear()
    return java_env.resolve()


def spark_env(env: dict | None = None) -> dict:
    return java_env.apply_to_env(env)
