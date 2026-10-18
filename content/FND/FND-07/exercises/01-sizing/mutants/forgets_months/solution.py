"""Mutant: storage_cost_usd treats $/GB-MONTH as $/GB-YEAR, forgetting the x12 for
months. The cost is ~12x too low -- a classic unit slip, far outside +/-20%."""

from __future__ import annotations

SECONDS_PER_YEAR = 3.1536e7


def throughput_bytes_per_sec(events_per_sec, avg_event_bytes) -> float:
    return events_per_sec * avg_event_bytes


def raw_bytes_per_year(events_per_sec, avg_event_bytes) -> float:
    return throughput_bytes_per_sec(events_per_sec, avg_event_bytes) * SECONDS_PER_YEAR


def stored_bytes_per_year(events_per_sec, avg_event_bytes,
                          compression_ratio, replication_factor) -> float:
    raw = raw_bytes_per_year(events_per_sec, avg_event_bytes)
    return raw / compression_ratio * replication_factor


def storage_cost_usd(stored_bytes, usd_per_gb_month, years) -> float:
    # BUG: forgets to multiply months by 12; uses years directly.
    return (stored_bytes / 1e9) * usd_per_gb_month * years


def query_cost_usd(bytes_scanned, usd_per_tb_scanned) -> float:
    return (bytes_scanned / 1e12) * usd_per_tb_scanned


def fits_on_one_box(working_set_bytes, machine_ram_bytes, headroom=0.5) -> bool:
    return working_set_bytes <= machine_ram_bytes * headroom


def assumptions() -> dict:
    return {"seconds_per_year": SECONDS_PER_YEAR,
            "compression_ratio_note": "x", "replication_note": "x"}
