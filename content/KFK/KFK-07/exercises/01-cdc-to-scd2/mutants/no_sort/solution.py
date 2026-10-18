"""Mutant: fold events in the order they arrive, WITHOUT sorting per key by
(ts_ms, lsn). On a shuffled stream this closes/opens versions out of order and
produces wrong validity windows (and wrong current rows)."""


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
        # BUG: no sort — process in arrival order
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
                if current is not None:
                    current["valid_to"] = ts
                    current["is_current"] = False
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
