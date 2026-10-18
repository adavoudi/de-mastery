Subtracts 1 after the modulo, so keys hashing to 0 get partition -1 — an off-by-one/sign bug that puts rows outside [0, p).
