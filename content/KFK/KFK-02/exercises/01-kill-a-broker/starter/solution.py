"""Starter — not implemented yet (must FAIL the tests: gate G1). See README.md."""


def write_accepted(acks, min_isr, isr_size):
    # TODO: acks 0/1 accepted when isr_size>=1; acks 'all' when isr_size>=min_isr.
    return False


def durable_after_one_more_loss(acks, min_isr, isr_size):
    # TODO: only acks='all' accepted into an ISR of size >= 2 survives one more loss.
    return False


def produce_and_verify(bootstrap_servers, topic, records, *, acks):
    # TODO: produce with the given acks, count deliveries, then read back.
    return 0
