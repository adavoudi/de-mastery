# KFK-12 · Security: SASL/SCRAM authentication and ACL authorization

## Why this module exists

An unsecured Kafka cluster is a database with no login and no `GRANT`. Anyone who can reach the
broker port can read every topic and write anything. For a SOX-regulated data platform that is a
non-starter: you must be able to prove **who** did what (authentication) and that each principal can
touch **only** what they are entitled to (authorization, least privilege). Kafka's security has two
independent layers that map exactly onto that:

- **Authentication — "who are you?"** The broker proves the client's identity. Options: TLS mutual
  auth (client certificates), or **SASL** mechanisms — `SCRAM-SHA-256/512` (salted
  username/password, credentials stored in the cluster metadata), `GSSAPI` (Kerberos), or
  `OAUTHBEARER` (tokens). The client ends up with a **principal** like `User:app`.
- **Authorization — "what may you do?"** An **authorizer** checks each request against **ACLs**:
  rules of the form *principal P is ALLOWED/DENIED operation O on resource R*. In KRaft the built-in
  authorizer is `StandardAuthorizer`; **super users** bypass ACLs; by default anything without a
  matching allow ACL is **denied**.

This is the Kafka version of your SOX access model: an authenticated identity, and an explicit grant
per object, with deny-by-default.

## How you lock down a cluster (objective ccaak:2)

**1. Turn on an authenticated listener.** Add a listener whose security protocol is
`SASL_PLAINTEXT` (dev) or `SASL_SSL` (prod), enable a mechanism
(`sasl.enabled.mechanisms=SCRAM-SHA-256`), and tell the broker where its own JAAS config is. Clients
connect to that listener with `security.protocol=SASL_...`, `sasl.mechanism=SCRAM-SHA-256`, and
their `sasl.username` / `sasl.password`.

**2. Create SCRAM credentials.** SCRAM users live in the cluster metadata, created with
`kafka-configs.sh --alter --add-config 'SCRAM-SHA-256=[password=...]' --entity-type users
--entity-name app`. There is no password sent in the clear on the wire — SCRAM does a
challenge-response.

**3. Turn on the authorizer and set super users.** `authorizer.class.name=...StandardAuthorizer`
and `super.users=User:...`. A trusted internal listener's principal is often made a super user so
cluster-management traffic is not blocked; application principals are **not** super users and must
earn every permission through an ACL.

**4. Grant least privilege with ACLs.** `kafka-acls.sh --add --allow-principal User:app
--operation Write --operation Describe --topic orders` lets `app` produce to `orders` and nothing
else. A principal with no matching allow ACL that produces to a topic gets
`TOPIC_AUTHORIZATION_FAILED`. That denial is the thing you *test for* — a security control you cannot
demonstrate failing is not a control.

Common operations: `Read`/`Write`/`Describe` on a **Topic**; `Read` on a **Group** (a consumer needs
both topic `Read` and group `Read`); `IdempotentWrite`/`Write` on the **Cluster**/**TransactionalId**
for an idempotent or transactional producer.

## From your world

| SOX / warehouse access control | Kafka security |
|---|---|
| A database login (user + password / SSO) | **SASL/SCRAM** authentication → a `User:` principal |
| `GRANT SELECT ON schema.table TO role` | an **ACL**: allow `Read` on a topic to a principal |
| Deny-by-default, explicit grants only | authorizer **denies** anything without an allow ACL |
| DBA / break-glass account | **super users** (bypass ACLs) — used sparingly |
| SOX evidence: "prove unauthorized access is blocked" | a test that a disallowed principal is **denied** |
| Least privilege per service account | one ACL per operation per topic, nothing extra |

## When NOT to / pitfalls

- **Don't make application principals super users.** Super users bypass every ACL — that is the
  opposite of least privilege. Reserve it for the trusted internal/admin path.
- **Don't rely on `SASL_PLAINTEXT` in production.** It authenticates but does not encrypt; use
  `SASL_SSL` so credentials and data are not on the wire in the clear. (The native lab uses
  `SASL_PLAINTEXT` purely to keep the exercise certificate-free.)
- **Don't forget the group ACL for consumers.** A consumer needs `Read` on the topic **and** `Read`
  on its consumer group; granting only the topic silently fails the group join.
- **Don't grant topic-wide `All`** when the service only produces — grant `Write`+`Describe`, not
  `Read`/`Delete`/`Alter`.

## Further reading

Official docs only:

- Security overview & authorization (ACLs):
  <https://kafka.apache.org/documentation/#security_authz>
- SASL/SCRAM:
  <https://kafka.apache.org/documentation/#security_sasl_scram>
- `kafka-acls.sh` CLI:
  <https://kafka.apache.org/documentation/#security_authz_cli>
- Configuring SCRAM credentials:
  <https://kafka.apache.org/documentation/#security_sasl_scram_credentials>
- Confluent CCAAK (security domain):
  <https://www.confluent.io/certification/>
