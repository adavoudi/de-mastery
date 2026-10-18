the phase-2 upsert omits the new region column, so evolved rows carry NULL region and the table fails to reconcile with the source.
