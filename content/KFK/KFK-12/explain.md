# KFK-12 · Explain it back (Feynman)

**Prompt A.** "Describe how you lock down a cluster so that `User:app` can produce to `orders` and
`User:evil` cannot. Cover both layers (authentication and authorization). Say where the SCRAM
credential lives. Say why the authorizer denies by default. Say which error `evil` gets. Map each step
to the SOX access-control model that you already use."

**Prompt B.** "A security reviewer asks you to *prove* that the control works. Which two assertions
make a convincing test? Why is 'the allowed path works' not enough on its own?"

## Rubric — a strong explanation covers these points (5–7):

- [ ] Separates authentication (SASL/SCRAM → a `User:` principal) from authorization (ACLs).
- [ ] States that SCRAM credentials live in cluster metadata and are not sent in the clear.
- [ ] Explains deny-by-default: no matching allow ACL ⇒ request denied.
- [ ] Gives the least-privilege grant (Write+Describe on exactly that topic) for a producer.
- [ ] Names the denial error (`TOPIC_AUTHORIZATION_FAILED`) that `evil` receives.
- [ ] Argues that the test must assert BOTH that an allowed principal succeeds AND that a disallowed
      one is denied.
- [ ] Maps to SOX: authenticated identity + explicit grant per object + deny-by-default.
