# FLK-07 · Explain it back (Feynman)

**Prompt A.** "Explain how you build a session timeout with a KeyedProcessFunction: what state you
keep, how timers express 'end the session after a 30-minute gap', why you delete the previous timer,
and what onTimer does. Why is this deterministic while a Thread.sleep approach is not?"

**Prompt B.** "A fraud threshold arrives on a broadcast stream and changes over time. Explain the
broadcast state pattern, the ordering trap between the broadcast and keyed inputs, and how you make
the job apply 'the rule in force on the event's date' both correctly and deterministically."

## Rubric — a strong explanation hits these (5–7):

- [ ] Names keyed state (count/sum/last-seen) and event-time timers as the session mechanism.
- [ ] Explains register-at-(event+gap) and delete-previous so only the latest event's timer fires.
- [ ] Explains onTimer emits the session result and clears state.
- [ ] Explains broadcast state replicates a control stream to every parallel instance.
- [ ] Identifies the ordering trap: broadcast and keyed inputs are unordered, so immediate evaluation
      is run-dependent.
- [ ] Explains the as-of fix: version rules by effective time, buffer events, evaluate on an
      event-time timer against the rule effective at or before the event time.
- [ ] Explains event-time timers are deterministic/reconcilable, unlike processing-time or sleep.
