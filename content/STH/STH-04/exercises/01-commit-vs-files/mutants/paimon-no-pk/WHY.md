Paimon target has no primary key, so it is an append-only table whose LSM never merges by key; every change event is kept and the Paimon result disagrees with the current-state oracle.
