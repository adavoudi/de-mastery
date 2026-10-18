def scan(rows):
    return list(rows)


def group_sum(rows, key, val):
    totals = {}
    for r in rows:
        totals[r[key]] = totals.get(r[key], 0) + 1  # WRONG: counts rows, ignores val
    return [{key: k, val: t} for k, t in totals.items()]
