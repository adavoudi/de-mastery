"""Alt 2 (different engine): use pandas to sort/group and shift() to close each
version with the next event's timestamp. Deletes are dropped from the version
rows but their timestamp still closes the preceding version via the shift."""


def _real(ev):
    return "op" in ev and ev.get("value", "__") is not None


def derive_scd2(events):
    import pandas as pd

    rows = []
    for ev in events:
        if not _real(ev):
            continue
        img = ev["after"] if ev["after"] is not None else ev["before"]
        rows.append({
            "id": img["id"],
            "op": ev["op"],
            "name": (ev["after"] or {}).get("name"),
            "tier": (ev["after"] or {}).get("tier"),
            "ts": ev["source"]["ts_ms"],
            "lsn": ev["source"]["lsn"],
        })
    if not rows:
        return []

    df = pd.DataFrame(rows).sort_values(["id", "ts", "lsn"]).reset_index(drop=True)
    # next event's ts within the key closes the current version
    df["valid_to"] = df.groupby("id")["ts"].shift(-1)
    df = df[df["op"] != "d"].copy()  # deletes open no version
    df["is_current"] = df["valid_to"].isna()

    out = []
    for _, r in df.iterrows():
        out.append({
            "id": int(r["id"]),
            "name": r["name"],
            "tier": r["tier"],
            "valid_from": int(r["ts"]),
            "valid_to": None if pd.isna(r["valid_to"]) else int(r["valid_to"]),
            "is_current": bool(r["is_current"]),
        })
    out.sort(key=lambda x: (x["id"], x["valid_from"]))
    return out
