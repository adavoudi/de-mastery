"""de_testkit: shared pytest fixtures and assertions for exercise tests."""

from de_testkit.asserts import (
    assert_frames_equal,
    assert_plan_contains,
    count_exchanges,
    plan_of,
)
from de_testkit.streaming import FileStreamSource, drive_available
from de_testkit.submission import import_submission
from de_testkit.avro_compat import check_compatibility
from de_testkit.java_toolkit import run_junit

__all__ = [
    "assert_frames_equal",
    "assert_plan_contains",
    "count_exchanges",
    "plan_of",
    "import_submission",
    "FileStreamSource",
    "drive_available",
    "check_compatibility",
    "run_junit",
]
