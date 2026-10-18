fix_advertised_listeners returns the config unchanged, so the unreachable advertised address is never corrected and clients still can't connect.
