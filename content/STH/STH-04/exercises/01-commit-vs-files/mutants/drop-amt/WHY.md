The apply statement drops the amt column (inserts NULL), so the mirrored amounts are all NULL and the table fails reconciliation against the oracle.
