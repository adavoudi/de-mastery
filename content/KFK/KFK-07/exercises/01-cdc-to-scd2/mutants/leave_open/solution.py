"""Mutant: on update, open a new current version but DO NOT close the old one.
Leaves two 'current' rows per key (valid_to stays None on the superseded row)."""


def _real(ev):
    return "op" in ev and ev.get("value", "__") is not None


def derive_scd2(events):
    by_key = {}
    for ev in events:
        if not _real(ev):
            continue
        row = ev["after"] if ev["after"] is not None else ev["before"]
        by_key.setdefault(row["id"], []).append(ev)

    out = []
    for key, evs in by_key.items():
        evs.sort(key=lambda e: (e["source"]["ts_ms"], e["source"]["lsn"]))
        current = None
        for ev in evs:
            ts = ev["source"]["ts_ms"]
            op = ev["op"]
            if op in ("r", "c"):
                current = {"id": key, "name": ev["after"]["name"],
                           "tier": ev["after"]["tier"], "valid_from": ts,
                           "valid_to": None, "is_current": True}
                out.append(current)
            elif op == "u":
                # BUG: open a new version without closing the previous one
                current = {"id": key, "name": ev["after"]["name"],
                           "tier": ev["after"]["tier"], "valid_from": ts,
                           "valid_to": None, "is_current": True}
                out.append(current)
            elif op == "d":
                if current is not None:
                    current["valid_to"] = ts
                    current["is_current"] = False
                current = None

    out.sort(key=lambda r: (r["id"], r["valid_from"]))
    return out
