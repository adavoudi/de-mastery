# KFK-12 · Interview drills

**1. "Authentication vs authorization in Kafka — one sentence each."**
> Authentication proves *who* the client is (TLS mutual auth or a SASL mechanism such as SCRAM),
> producing a `User:` principal. Authorization decides *what* that principal may do, by matching each
> request against ACLs in the authorizer. Separate layers; you need both.

**2. "How do you create a SCRAM user and grant it produce-only on one topic?"**
> `kafka-configs.sh --alter --add-config 'SCRAM-SHA-256=[password=...]' --entity-type users
> --entity-name app` creates the credential in cluster metadata. Then
> `kafka-acls.sh --add --allow-principal User:app --operation Write --operation Describe --topic
> orders` grants exactly produce rights on that topic — nothing else.

**3. "What error does a disallowed producer get, and why do you test for it?"**
> `TOPIC_AUTHORIZATION_FAILED`. You test for the *denial* because a security control you can only
> show working on the happy path isn't proven — the whole point is that unauthorized access is
> blocked. 'Allowed works' + 'disallowed is denied' together demonstrate the control.

**4. "Why not just make every service a super user to avoid ACL headaches?"**
> Super users bypass all ACLs, so you lose least privilege and auditability — one leaked credential
> then has full cluster access. Super user is for a trusted internal/admin path only; apps get
> narrow, explicit grants.

**5. "A consumer authenticates fine and has topic Read but can't consume. What's wrong?"**
> It's missing `Read` on the consumer **Group** resource. Consuming needs topic Read *and* group
> Read; without the group grant the join/commit is denied even though fetches would authorize.

**6. "SASL_PLAINTEXT vs SASL_SSL — when is each OK?"**
> Both authenticate via SASL. `SASL_PLAINTEXT` does not encrypt, so it's only acceptable on a trusted
> local/loopback link (or inside a secured network segment). Production uses `SASL_SSL` so
> credentials and payloads are encrypted in transit.
