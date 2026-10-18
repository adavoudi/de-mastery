"""Alt 1 (genuinely different structure): build each key's ordered timeline, then
derive versions in a second pass using the NEXT event's timestamp to close the
current one — a 'look-ahead' fold rather than a running-current mutable state."""


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
        evs = sorted(evs, key=lambda e: (e["source"]["ts_ms"], e["source"]["lsn"]))
        for i, ev in enumerate(evs):
            op = ev["op"]
            if op == "d":
                continue  # a delete opens no version
            after = ev["after"]
            valid_from = ev["source"]["ts_ms"]
            # closed by the NEXT event (if any) for this key
            valid_to = evs[i + 1]["source"]["ts_ms"] if i + 1 < len(evs) else None
            out.append({
                "id": key,
                "name": after["name"],
                "tier": after["tier"],
                "valid_from": valid_from,
                "valid_to": valid_to,
                "is_current": valid_to is None,
            })
    out.sort(key=lambda r: (r["id"], r["valid_from"]))
    return out
