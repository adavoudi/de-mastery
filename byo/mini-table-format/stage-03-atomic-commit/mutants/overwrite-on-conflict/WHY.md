On a lost put_if_absent it falls back to an unconditional put(), overwriting the concurrent writer's version (lost update).
