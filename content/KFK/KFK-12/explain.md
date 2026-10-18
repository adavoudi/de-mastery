# KFK-12 · Explain it back (Feynman)

**Prompt A.** "Walk through locking down a cluster so that `User:app` can produce to `orders` and
`User:evil` cannot. Cover both layers (authentication and authorization), where the SCRAM credential
lives, why the authorizer denies by default, and what error `evil` gets. Map each step to the SOX
access-control model you already use."

**Prompt B.** "A security reviewer asks you to *prove* the control works. What two assertions make a
convincing test, and why is 'the allowed path works' not enough on its own?"

## Rubric — a strong explanation hits these (5–7):

- [ ] Separates authentication (SASL/SCRAM → a `User:` principal) from authorization (ACLs).
- [ ] States that SCRAM credentials live in cluster metadata and are not sent in the clear.
- [ ] Explains deny-by-default: no matching allow ACL ⇒ request denied.
- [ ] Gives the least-privilege grant (Write+Describe on exactly that topic) for a producer.
- [ ] Names the denial error (`TOPIC_AUTHORIZATION_FAILED`) that `evil` receives.
- [ ] Argues the test must assert BOTH an allowed principal succeeds AND a disallowed one is denied.
- [ ] Maps to SOX: authenticated identity + explicit per-object grant + deny-by-default.
