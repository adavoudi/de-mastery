"""Alt solution 2 for FND-07: uses the rounded 3.15e7 seconds/year and named SI unit
constants, computing costs via explicit unit conversions. Different constant choice,
same model within tolerance."""

from __future__ import annotations

SECONDS_PER_YEAR = 3.15e7
MB = 1e6
GB = 1e9
TB = 1e12
MONTHS_PER_YEAR = 12


def throughput_bytes_per_sec(events_per_sec, avg_event_bytes) -> float:
    return float(events_per_sec * avg_event_bytes)


def raw_bytes_per_year(events_per_sec, avg_event_bytes) -> float:
    bps = throughput_bytes_per_sec(events_per_sec, avg_event_bytes)
    return bps * SECONDS_PER_YEAR


def stored_bytes_per_year(events_per_sec, avg_event_bytes,
                          compression_ratio, replication_factor) -> float:
    raw = raw_bytes_per_year(events_per_sec, avg_event_bytes)
    compressed = raw / compression_ratio
    return compressed * replication_factor


def storage_cost_usd(stored_bytes, usd_per_gb_month, years) -> float:
    gb_stored = stored_bytes / GB
    total_months = years * MONTHS_PER_YEAR
    return gb_stored * usd_per_gb_month * total_months


def query_cost_usd(bytes_scanned, usd_per_tb_scanned) -> float:
    return (bytes_scanned / TB) * usd_per_tb_scanned


def fits_on_one_box(working_set_bytes, machine_ram_bytes, headroom=0.5) -> bool:
    return working_set_bytes <= headroom * machine_ram_bytes


def assumptions() -> dict:
    return {
        "seconds_per_year": SECONDS_PER_YEAR,
        "compression_ratio_note": "3x columnar; divides raw bytes",
        "replication_note": "3x copies; multiplies stored bytes",
        "unit_system": "SI (MB=1e6, GB=1e9, TB=1e12)",
    }
