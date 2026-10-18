from collections import defaultdict


def scan(rows):
    return [dict(r) for r in rows]


def group_sum(rows, key, val):
    acc = defaultdict(int)
    for r in rows:
        acc[r[key]] += r[val]
    return [{key: k, val: t} for k, t in acc.items()]
