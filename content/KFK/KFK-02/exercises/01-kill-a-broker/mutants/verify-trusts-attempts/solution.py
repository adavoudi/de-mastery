"""Mutant: verify-trusts-attempts — produce_and_verify returns len(records)
without consuming anything back, so it 'verifies' durability it never checked.
When the live read-back would be short (or the delivery count differs), this is
wrong."""


def write_accepted(acks, min_isr, isr_size):
    acks = str(acks)
    if isr_size < 1:
        return False
    if acks in ("0", "1"):
        return True
    return isr_size >= min_isr


def durable_after_one_more_loss(acks, min_isr, isr_size):
    acks = str(acks)
    if not write_accepted(acks, min_isr, isr_size):
        return False
    return acks == "all" and min_isr >= 2 and isr_size >= 2


def produce_and_verify(bootstrap_servers, topic, records, *, acks):
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": bootstrap_servers, "acks": str(acks),
                  "retries": 5, "message.timeout.ms": 10000})
    for k, v in records:
        p.produce(topic, key=k, value=v)
    p.flush(15)
    # BUG: assumes every attempted record is durably present; never reads back.
    return len(records)
