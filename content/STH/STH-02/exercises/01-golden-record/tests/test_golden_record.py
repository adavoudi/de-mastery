"""STH-02/01 — Paimon partial-update customer golden record (MDM).

The learner authors the table DDL (partial-update PK table) and the per-source
INSERTs. We run them through the native embedded Paimon runner (no Docker) and
reconcile the merged golden records against an INDEPENDENT Python oracle that
computes the per-column latest-non-null a different way (G6).

SKIPs cleanly when the vendored Flink/JDK/Paimon jars are absent (G1 caveat).
"""

import pytest

# --- deterministic, bounded source feeds -----------------------------------
# customer 1: known to all three sources -> full golden record.
# customer 2: known to CRM + billing only -> phone stays NULL.
# customer 3: known to loyalty only -> only phone.
# customer 4: billing sends email, then a later CRM row with name AND an
#             explicit NULL email must NOT erase the email (latest-non-null).
CRM = [(1, "Alice"), (2, "Bob"), (4, "Dana")]
BILLING = [(1, "alice@x.com"), (2, "bob@x.com"), (4, "dana@x.com")]
LOYALTY = [(1, "555-0001"), (3, "555-0003")]


def _oracle():
    """Independent per-column latest-non-null golden record, order of arrival
    = CRM then billing then loyalty (matches the insert order the solution emits).
    Written as an explicit fold so it does not share code with any solution."""
    cols = {"name": {}, "email": {}, "phone": {}}
    # apply in the SAME order the inserts are emitted (crm, billing, loyalty)
    for cid, name in CRM:
        cols["name"][cid] = name  # non-null wins; CRM never sends NULL name here
    for cid, email in BILLING:
        cols["email"][cid] = email
    for cid, phone in LOYALTY:
        cols["phone"][cid] = phone
    ids = sorted({c for c, _ in CRM} | {c for c, _ in BILLING} | {c for c, _ in LOYALTY})
    out = []
    for cid in ids:
        out.append([
            str(cid),
            cols["name"].get(cid),
            cols["email"].get(cid),
            cols["phone"].get(cid),
        ])
    return out


def _run(paimon_sql, submission, table="cust"):
    ddl = submission.table_ddl(table)
    inserts = submission.source_inserts(table, CRM, BILLING, LOYALTY)
    rows = paimon_sql.batch(
        ddl=[ddl],
        inserts=inserts,
        query=f"SELECT customer_id, name, email, phone FROM {table} ORDER BY customer_id",
        timeout_s=180,
    )
    # strip the RowKind column; normalise fields to str/None
    out = []
    for r in rows:
        fields = r[1:]
        out.append([None if f is None else str(f) for f in fields])
    return out


@pytest.mark.timeout(240)
def test_golden_record_matches_oracle(paimon_sql, submission):
    got = _run(paimon_sql, submission)
    want = _oracle()
    assert got == want, f"\n got={got}\nwant={want}"


@pytest.mark.timeout(240)
def test_full_record_when_all_three_sources(paimon_sql, submission):
    # customer 1 is known to CRM, billing and loyalty: all three fields present.
    got = {r[0]: r for r in _run(paimon_sql, submission)}
    assert got["1"] == ["1", "Alice", "alice@x.com", "555-0001"], got.get("1")


@pytest.mark.timeout(240)
def test_missing_source_leaves_null(paimon_sql, submission):
    # customer 2 has no loyalty row -> phone stays NULL; customer 3 only loyalty.
    got = {r[0]: r for r in _run(paimon_sql, submission)}
    assert got["2"] == ["2", "Bob", "bob@x.com", None], got.get("2")
    assert got["3"] == ["3", None, None, "555-0003"], got.get("3")


@pytest.mark.timeout(240)
def test_later_null_does_not_erase(paimon_sql, submission):
    # customer 4: billing set the email; the CRM row (name + NULL email) must NOT
    # wipe the email out. This is the partial-update latest-non-null guarantee and
    # the core MDM data-loss check (kills the deduplicate / overwrite mutants).
    got = {r[0]: r for r in _run(paimon_sql, submission)}
    assert got["4"] == ["4", "Dana", "dana@x.com", None], got.get("4")
