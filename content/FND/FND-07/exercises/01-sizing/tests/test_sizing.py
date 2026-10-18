"""Tests for FND-07/01-sizing.

Each numeric estimate is checked within +/-20% of an INDEPENDENT model the test
computes from the same stated assumptions. A Hypothesis monotonicity check catches
sign/operation errors without pinning exact values.
"""

from __future__ import annotations

import math

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

TOL = 0.20  # +/-20%
MODEL_SPY = 3.1536e7  # seconds/year used by the model


def _close(actual, expected, tol=TOL):
    if expected == 0:
        return abs(actual) <= 1e-9
    return abs(actual - expected) / abs(expected) <= tol


# --- independent model ------------------------------------------------------
def _m_throughput(e, b):
    return e * b


def _m_raw_year(e, b):
    return e * b * MODEL_SPY


def _m_stored_year(e, b, comp, repl):
    return _m_raw_year(e, b) / comp * repl


def _m_storage_cost(stored, usd_gb_mo, years):
    return (stored / 1e9) * usd_gb_mo * (years * 12)


def _m_query_cost(scanned, usd_tb):
    return (scanned / 1e12) * usd_tb


# ---------------------------------------------------------------------------
SCENARIOS = [
    # (events_per_sec, avg_event_bytes, compression, replication)
    (2_000_000, 100, 3.0, 3.0),   # ~5-6 PB/year headline case
    (1, 1000, 1.0, 1.0),
    (50_000, 512, 4.0, 2.0),
    (10, 2_000_000, 10.0, 3.0),
]


def test_throughput_within_tolerance(submission):
    for e, b, _, _ in SCENARIOS:
        got = submission.throughput_bytes_per_sec(e, b)
        assert _close(got, _m_throughput(e, b)), (e, b, got)


def test_raw_bytes_per_year_within_tolerance(submission):
    for e, b, _, _ in SCENARIOS:
        got = submission.raw_bytes_per_year(e, b)
        assert _close(got, _m_raw_year(e, b)), (e, b, got)


def test_stored_bytes_per_year_within_tolerance(submission):
    for e, b, comp, repl in SCENARIOS:
        got = submission.stored_bytes_per_year(e, b, comp, repl)
        assert _close(got, _m_stored_year(e, b, comp, repl)), (e, b, comp, repl, got)


def test_headline_case_is_about_5pb_per_year(submission):
    # 2M events/sec, 100 B, 3x compression, 3x replication -> ~6.3 PB/year on disk.
    stored = submission.stored_bytes_per_year(2_000_000, 100, 3.0, 3.0)
    pb = stored / 1e15
    assert 4.0 <= pb <= 8.0, f"expected a few PB/year, got {pb:.2f} PB"


def test_storage_cost_within_tolerance(submission):
    cases = [(5e15, 0.023, 7), (1e12, 0.02, 1), (2.1e15, 0.025, 3)]
    for stored, rate, years in cases:
        got = submission.storage_cost_usd(stored, rate, years)
        assert _close(got, _m_storage_cost(stored, rate, years)), (stored, rate, years, got)


def test_query_cost_within_tolerance(submission):
    cases = [(1e12, 5.0), (5e11, 5.0), (3e13, 6.25)]
    for scanned, rate in cases:
        got = submission.query_cost_usd(scanned, rate)
        assert _close(got, _m_query_cost(scanned, rate)), (scanned, rate, got)


def test_fits_on_one_box_boundary(submission):
    ram = 32 * 10**9  # 32 GB
    # Default headroom 0.5 -> usable 16 GB.
    assert submission.fits_on_one_box(15 * 10**9, ram) is True
    assert submission.fits_on_one_box(16 * 10**9, ram) is True   # exactly at boundary
    assert submission.fits_on_one_box(20 * 10**9, ram) is False
    # A 0.9 headroom accepts more.
    assert submission.fits_on_one_box(28 * 10**9, ram, headroom=0.9) is True
    assert submission.fits_on_one_box(30 * 10**9, ram, headroom=0.9) is False


def test_assumptions_exposes_required_keys(submission):
    a = submission.assumptions()
    for key in ("seconds_per_year", "compression_ratio_note", "replication_note"):
        assert key in a, f"assumptions() missing {key!r}"
    assert _close(float(a["seconds_per_year"]), MODEL_SPY)


# ---------------------------------------------------------------------------
# Hypothesis: monotonicity (catches divide/multiply/sign slips)
# ---------------------------------------------------------------------------
@settings(max_examples=100, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(e=st.integers(min_value=1, max_value=10_000_000),
       b=st.integers(min_value=1, max_value=10_000),
       de=st.integers(min_value=1, max_value=1_000_000))
def test_more_events_never_decreases_throughput(submission, e, b, de):
    assert (submission.throughput_bytes_per_sec(e + de, b)
            >= submission.throughput_bytes_per_sec(e, b))


@settings(max_examples=100, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(stored=st.floats(min_value=1e9, max_value=1e18),
       rate=st.floats(min_value=0.001, max_value=1.0),
       years=st.integers(min_value=1, max_value=10),
       dy=st.integers(min_value=1, max_value=10))
def test_more_years_never_decreases_storage_cost(submission, stored, rate, years, dy):
    more = submission.storage_cost_usd(stored, rate, years + dy)
    base = submission.storage_cost_usd(stored, rate, years)
    assert more >= base or math.isclose(more, base, rel_tol=1e-9)
