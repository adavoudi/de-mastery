"""Reference solution for FND-07/01-sizing.

Back-of-envelope sizing/cost. Returns numbers checked within +/-20% by the tests.
"""

from __future__ import annotations

SECONDS_PER_YEAR = 3.1536e7  # 365 days


def throughput_bytes_per_sec(events_per_sec, avg_event_bytes) -> float:
    return float(events_per_sec) * float(avg_event_bytes)


def raw_bytes_per_year(events_per_sec, avg_event_bytes) -> float:
    return throughput_bytes_per_sec(events_per_sec, avg_event_bytes) * SECONDS_PER_YEAR


def stored_bytes_per_year(events_per_sec, avg_event_bytes,
                          compression_ratio, replication_factor) -> float:
    raw = raw_bytes_per_year(events_per_sec, avg_event_bytes)
    return raw / float(compression_ratio) * float(replication_factor)


def storage_cost_usd(stored_bytes, usd_per_gb_month, years) -> float:
    gb = float(stored_bytes) / 1e9
    months = float(years) * 12.0
    return gb * float(usd_per_gb_month) * months


def query_cost_usd(bytes_scanned, usd_per_tb_scanned) -> float:
    tb = float(bytes_scanned) / 1e12
    return tb * float(usd_per_tb_scanned)


def fits_on_one_box(working_set_bytes, machine_ram_bytes, headroom=0.5) -> bool:
    return float(working_set_bytes) <= float(machine_ram_bytes) * float(headroom)


def assumptions() -> dict:
    return {
        "seconds_per_year": SECONDS_PER_YEAR,
        "compression_ratio_note": "columnar ~3x; divides raw size",
        "replication_note": "3x copies for durability; multiplies stored size",
    }
