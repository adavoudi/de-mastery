"""Reference: fold a Debezium change stream into SCD2 versions.

Order per key by commit (ts_ms, lsn), skip tombstones, and apply:
  r/c -> open a new current version
  u   -> close the current version and open a new one
  d   -> close the current version, open nothing
"""


def _is_tombstone(ev):
    # A tombstone has a null value and no change envelope.
    return ev.get("value", "__") is None or ("op" not in ev)


def derive_scd2(events):
    # keep only real change envelopes
    changes = [ev for ev in events if not _is_tombstone(ev)]

    # group by key id (from after, else before)
    by_key = {}
    for ev in changes:
        row = ev["after"] if ev["after"] is not None else ev["before"]
        key = row["id"]
        by_key.setdefault(key, []).append(ev)

    out = []
    for key, evs in by_key.items():
        evs.sort(key=lambda e: (e["source"]["ts_ms"], e["source"]["lsn"]))
        current = None  # the open version dict, or None
        for ev in evs:
            ts = ev["source"]["ts_ms"]
            op = ev["op"]
            if op in ("r", "c"):
                after = ev["after"]
                current = {
                    "id": key, "name": after["name"], "tier": after["tier"],
                    "valid_from": ts, "valid_to": None, "is_current": True,
                }
                out.append(current)
            elif op == "u":
                if current is not None:
                    current["valid_to"] = ts
                    current["is_current"] = False
                after = ev["after"]
                current = {
                    "id": key, "name": after["name"], "tier": after["tier"],
                    "valid_from": ts, "valid_to": None, "is_current": True,
                }
                out.append(current)
            elif op == "d":
                if current is not None:
                    current["valid_to"] = ts
                    current["is_current"] = False
                current = None

    out.sort(key=lambda r: (r["id"], r["valid_from"]))
    return out
