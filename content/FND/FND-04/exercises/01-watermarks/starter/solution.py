"""Starter for FND-04/01-watermarks. See README.md."""

from __future__ import annotations

NEG_INF = float("-inf")


class WatermarkGenerator:
    def __init__(self, allowed_lateness: int):
        # TODO: track max_seen and a monotonic watermark.
        raise NotImplementedError

    def observe(self, event_time: int) -> int:
        raise NotImplementedError

    def current(self) -> int:
        raise NotImplementedError


def classify_events(events, allowed_lateness: int, grace=None) -> list[dict]:
    # TODO: classify each event against the PRE-event watermark, then observe it.
    raise NotImplementedError
