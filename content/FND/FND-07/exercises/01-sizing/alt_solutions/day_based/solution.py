"""Alt solution 1 for FND-07: derives seconds/year from 86400 * 365 and builds the
estimates by composing small unit helpers. Numerically within +/-20% of the model."""

from __future__ import annotations

SECONDS_PER_DAY = 86_400
DAYS_PER_YEAR = 365
SECONDS_PER_YEAR = SECONDS_PER_DAY * DAYS_PER_YEAR  # 3.1536e7


def _per_year(x_per_sec):
    return x_per_sec * SECONDS_PER_YEAR


def throughput_bytes_per_sec(events_per_sec, avg_event_bytes) -> float:
    return events_per_sec * avg_event_bytes


def raw_bytes_per_year(events_per_sec, avg_event_bytes) -> float:
    return _per_year(throughput_bytes_per_sec(events_per_sec, avg_event_bytes))


def stored_bytes_per_year(events_per_sec, avg_event_bytes,
                          compression_ratio, replication_factor) -> float:
    return (raw_bytes_per_year(events_per_sec, avg_event_bytes)
            * replication_factor / compression_ratio)


def storage_cost_usd(stored_bytes, usd_per_gb_month, years) -> float:
    gb = stored_bytes / 1_000_000_000
    return gb * usd_per_gb_month * (12 * years)


def query_cost_usd(bytes_scanned, usd_per_tb_scanned) -> float:
    return (bytes_scanned / 1_000_000_000_000) * usd_per_tb_scanned


def fits_on_one_box(working_set_bytes, machine_ram_bytes, headroom=0.5) -> bool:
    usable = machine_ram_bytes * headroom
    return not (working_set_bytes > usable)


def assumptions() -> dict:
    return {
        "seconds_per_year": SECONDS_PER_YEAR,
        "compression_ratio_note": "assume ~3x columnar compression (divides raw)",
        "replication_note": "assume 3x replication (multiplies stored)",
        "seconds_derivation": "86400 * 365",
    }
