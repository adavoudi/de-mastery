def scan(rows):
    return list(rows)


def group_sum(rows, key, val):
    totals = {}
    for r in rows:
        totals[r[key]] = r[val]  # WRONG: last value wins, no accumulation
    return [{key: k, val: t} for k, t in totals.items()]
