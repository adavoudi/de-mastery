def scan(rows):
    # genuinely different: comprehension copy instead of list()
    return [dict(r) for r in rows]


def group_sum(rows, key, val):
    raise NotImplementedError
