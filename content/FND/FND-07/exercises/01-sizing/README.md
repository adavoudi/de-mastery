# Size and cost a 5 PB/year pipeline (checked within ±20%)

This is an **estimation** exercise. You fill in functions that return numbers; the tests recompute
each number from the **same stated assumptions** and accept your answer if it is within **±20%**.
The tolerance exists on purpose: back-of-envelope math is about the right order of magnitude, so
rounding (e.g. `3.15e7` vs `3.1536e7` seconds/year) must not fail you. All units are **SI**
(1 MB = 1e6 bytes, 1 GB = 1e9, 1 TB = 1e12, 1 PB = 1e15).

## Stated assumptions (use exactly these; the tests use them too)

- `SECONDS_PER_YEAR ≈ 3.15e7` (365 days; you may use `3.1536e7`).
- Storage: **compression divides** raw size; **replication multiplies** it.
- Costs are parameters passed in (prices change; the method doesn't).

## Functions to implement in `solution.py`

### 1. `throughput_bytes_per_sec(events_per_sec, avg_event_bytes) -> float`
`events_per_sec × avg_event_bytes`.

### 2. `raw_bytes_per_year(events_per_sec, avg_event_bytes) -> float`
Throughput × `SECONDS_PER_YEAR`.

### 3. `stored_bytes_per_year(events_per_sec, avg_event_bytes, compression_ratio, replication_factor) -> float`
`raw_bytes_per_year / compression_ratio × replication_factor`.

### 4. `storage_cost_usd(stored_bytes, usd_per_gb_month, years) -> float`
Convert `stored_bytes` to GB (`/1e9`), multiply by `usd_per_gb_month` and by `years × 12` months.

### 5. `query_cost_usd(bytes_scanned, usd_per_tb_scanned) -> float`
`(bytes_scanned / 1e12) × usd_per_tb_scanned`.

### 6. `fits_on_one_box(working_set_bytes, machine_ram_bytes, headroom=0.5) -> bool`
Return `True` if the working set fits within `headroom` of RAM (default 50%): i.e.
`working_set_bytes <= machine_ram_bytes × headroom`. This is the FND-01 "one big box?" check.

### 7. `assumptions() -> dict`
Return the assumptions you used so the estimate is auditable. Must include the keys
`"seconds_per_year"`, `"compression_ratio_note"`, `"replication_note"` with sensible values /
short strings. (The test only checks these keys exist and `seconds_per_year` is within ±20% of
`3.15e7` — stating assumptions is part of the skill.)

## What the tests check

- Each numeric function is within **±20%** of an **independent model** the test computes from the
  same assumptions, across several input scenarios (including the 2M events/sec, 100 B/event,
  3× compression, 3× replication case that lands near **5 PB/year**).
- `fits_on_one_box` returns the correct boolean at and around the headroom boundary.
- `assumptions()` exposes the required keys and a sane `seconds_per_year`.
- A **monotonicity sanity check (Hypothesis):** more events, bytes, or years never *decreases* the
  corresponding estimate (catches sign/÷-vs-× mistakes without pinning exact values).

Constraints: pure Python, deterministic. `lab: none`.
