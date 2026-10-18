"""Starter for FND-07/01-sizing. Fill in each function. See README.md.

All units are SI (1 MB = 1e6 bytes). Estimates are checked within +/-20%.
"""

from __future__ import annotations

SECONDS_PER_YEAR = 3.15e7  # 365 days


def throughput_bytes_per_sec(events_per_sec, avg_event_bytes) -> float:
    # TODO: events/sec * bytes/event
    raise NotImplementedError


def raw_bytes_per_year(events_per_sec, avg_event_bytes) -> float:
    # TODO: throughput * SECONDS_PER_YEAR
    raise NotImplementedError


def stored_bytes_per_year(events_per_sec, avg_event_bytes,
                          compression_ratio, replication_factor) -> float:
    # TODO: raw / compression_ratio * replication_factor
    raise NotImplementedError


def storage_cost_usd(stored_bytes, usd_per_gb_month, years) -> float:
    # TODO: GB * $/GB-month * (years * 12)
    raise NotImplementedError


def query_cost_usd(bytes_scanned, usd_per_tb_scanned) -> float:
    # TODO: TB scanned * $/TB
    raise NotImplementedError


def fits_on_one_box(working_set_bytes, machine_ram_bytes, headroom=0.5) -> bool:
    # TODO: working_set <= ram * headroom
    raise NotImplementedError


def assumptions() -> dict:
    # TODO: return the assumptions you used (seconds_per_year, compression, replication).
    raise NotImplementedError
