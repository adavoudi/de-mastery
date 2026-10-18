# KFK-12 · Interview drills

**1. "Authentication vs authorization in Kafka — one sentence each."**
> Authentication proves *who* the client is (TLS mutual auth or a SASL mechanism such as SCRAM). It
> produces a `User:` principal. Authorization decides *what* that principal may do. The authorizer
> matches each request against ACLs. These are separate layers, and you need both.

**2. "How do you create a SCRAM user and grant it produce-only on one topic?"**
> `kafka-configs.sh --alter --add-config 'SCRAM-SHA-256=[password=...]' --entity-type users
> --entity-name app` creates the credential in cluster metadata. Then
> `kafka-acls.sh --add --allow-principal User:app --operation Write --operation Describe --topic
> orders` grants exactly the produce rights on that topic and nothing else.

**3. "What error does a disallowed producer get, and why do you test for it?"**
> It gets `TOPIC_AUTHORIZATION_FAILED`. You test for the *denial* because a security control is not
> proven if you show only the happy path. The whole point is that Kafka blocks unauthorized access.
> 'Allowed works' + 'disallowed is denied' together demonstrate the control.

**4. "Why not just make every service a super user to avoid ACL headaches?"**
> Super users bypass all ACLs. You lose least privilege and auditability. Then one leaked credential
> gives full cluster access. Use a super user only for a trusted internal/admin path. Give apps narrow,
> explicit grants.

**5. "A consumer authenticates fine and has topic Read but can't consume. What's wrong?"**
> It is missing `Read` on the consumer **Group** resource. Consuming needs topic Read *and* group Read.
> Without the group grant, Kafka denies the join and commit, even though it would authorize the
> fetches.

**6. "SASL_PLAINTEXT vs SASL_SSL — when is each OK?"**
> Both authenticate with SASL. `SASL_PLAINTEXT` does not encrypt. It is acceptable only on a trusted
> local/loopback link (or inside a secured network segment). Production uses `SASL_SSL`, so Kafka
> encrypts credentials and payloads in transit.
