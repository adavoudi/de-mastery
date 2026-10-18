The reduce step overwrites partials instead of summing them, so a product split across chunks keeps only the last worker's partial — a non-associative, worker-count-dependent combine.
