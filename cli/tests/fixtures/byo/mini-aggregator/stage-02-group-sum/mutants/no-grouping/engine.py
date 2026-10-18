def scan(rows):
    return list(rows)


def group_sum(rows, key, val):
    # WRONG: no grouping at all, returns each row's val as its own group
    return [{key: r[key], val: r[val]} for r in rows]
