# FLK-07 · Explain it back (Feynman)

**Prompt A.** "Explain how you build a session timeout with a KeyedProcessFunction. Say what state you
keep. Say how timers express 'end the session after a 30-minute gap'. Say why you delete the previous timer.
Say what onTimer does. Why is this deterministic when a Thread.sleep approach is not?"

**Prompt B.** "A fraud threshold arrives on a broadcast stream and changes over time. Explain the
broadcast state pattern. Explain the ordering trap between the broadcast input and the keyed input.
Explain how you make the job apply 'the rule in force on the date of the event' both correctly and deterministically."

## Rubric — a strong explanation hits these (5–7):

- [ ] Names keyed state (count/sum/last-seen) and event-time timers as the session mechanism.
- [ ] Explains register-at-(event+gap) and delete-previous, so only the timer of the latest event fires.
- [ ] Explains that onTimer emits the session result and clears the state.
- [ ] Explains that broadcast state replicates a control stream to every parallel instance.
- [ ] Identifies the ordering trap: the broadcast and keyed inputs are unordered, so immediate evaluation
      gives a result that depends on the run.
- [ ] Explains the as-of fix: version the rules by effective time, buffer events, and evaluate on an
      event-time timer against the rule that is effective at or before the event time.
- [ ] Explains that event-time timers are deterministic and reconcilable, unlike processing-time timers or sleep.
