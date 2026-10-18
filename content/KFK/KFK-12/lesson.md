# KFK-12 · Security: SASL/SCRAM authentication and ACL authorization

## Why this module exists

An unsecured Kafka cluster is a database with no login and no `GRANT`. Anyone who can reach the
broker port can read every topic and write anything. A SOX-regulated data platform cannot accept this.
You must be able to prove **who** did what (authentication). You must also prove that each principal
can touch **only** what it is entitled to touch (authorization, least privilege). Kafka security has
two independent layers that match these two needs:

- **Authentication — "who are you?"** The broker proves the identity of the client. The options are TLS
  mutual auth (client certificates) or **SASL** mechanisms:
  - `SCRAM-SHA-256/512`: salted username and password. The cluster stores the credentials in its
    metadata.
  - `GSSAPI`: Kerberos.
  - `OAUTHBEARER`: tokens.

  The client gets a **principal** such as `User:app`.
- **Authorization — "what may you do?"** An **authorizer** checks each request against **ACLs**. An ACL
  is a rule of this form: *principal P is ALLOWED/DENIED operation O on resource R*. In KRaft, the
  built-in authorizer is `StandardAuthorizer`. **Super users** bypass ACLs. By default, the authorizer
  **denies** anything without a matching allow ACL.

This is the Kafka version of your SOX access model: an authenticated identity, and an explicit grant
for each object, with deny-by-default.

## How you lock down a cluster

**1. Turn on an authenticated listener.**
- Add a listener whose security protocol is `SASL_PLAINTEXT` (dev) or `SASL_SSL` (prod).
- Enable a mechanism (`sasl.enabled.mechanisms=SCRAM-SHA-256`).
- Tell the broker where its own JAAS config is.

Clients connect to that listener with `security.protocol=SASL_...`, `sasl.mechanism=SCRAM-SHA-256`,
and their `sasl.username` and `sasl.password`.

**2. Create SCRAM credentials.** SCRAM users live in the cluster metadata. You create them with
`kafka-configs.sh --alter --add-config 'SCRAM-SHA-256=[password=...]' --entity-type users
--entity-name app`. The password is never sent in the clear on the wire, because SCRAM uses a
challenge-response.

**3. Turn on the authorizer and set super users.** Set `authorizer.class.name=...StandardAuthorizer`
and `super.users=User:...`. Often, you make the principal of a trusted internal listener a super user.
Then Kafka does not block the cluster-management traffic. Application principals are **not** super
users. They must earn every permission through an ACL.

**4. Grant least privilege with ACLs.** The command `kafka-acls.sh --add --allow-principal User:app
--operation Write --operation Describe --topic orders` lets `app` produce to `orders` and do nothing
else. Suppose a principal has no matching allow ACL and produces to a topic. It gets
`TOPIC_AUTHORIZATION_FAILED`. You *test for* this denial. If you cannot show that a security control
fails an unauthorized request, it is not a control.

Common operations:
- `Read`/`Write`/`Describe` on a **Topic**.
- `Read` on a **Group**. A consumer needs both topic `Read` and group `Read`.
- `IdempotentWrite`/`Write` on the **Cluster**/**TransactionalId**, for an idempotent or transactional
  producer.

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

- **Don't make application principals super users.** Super users bypass every ACL. This is the
  opposite of least privilege. Use super users only for the trusted internal/admin path.
- **Don't rely on `SASL_PLAINTEXT` in production.** It authenticates but does not encrypt. Use
  `SASL_SSL`, so credentials and data are not on the wire in the clear. (The native lab uses
  `SASL_PLAINTEXT` only to keep the exercise free of TLS certificates.)
- **Don't forget the group ACL for consumers.** A consumer needs `Read` on the topic **and** `Read` on
  its consumer group. If you grant only the topic, the group join fails without a clear message.
- **Don't grant `All` on the topic** when the service only produces. Grant `Write`+`Describe`, not
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
