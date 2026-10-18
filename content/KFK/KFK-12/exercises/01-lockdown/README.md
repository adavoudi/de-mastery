# KFK-12 · Lock down a cluster with SASL/SCRAM + ACLs

The grader starts a cluster with an **authorizer** (`StandardAuthorizer`) and a **SASL/SCRAM**
listener. Your job is to implement the functions that create credentials, grant a least-privilege ACL,
and build the client config. Then an **allowed** principal can produce and a **disallowed** principal
is **denied**. Edit `secure.py`.

## Functions to implement

### `scram_add_config(password) -> str` (pure)
Return the value that `kafka-configs.sh` expects after `--add-config` for a SCRAM-SHA-256 user,
exactly: `SCRAM-SHA-256=[password=<password>]`.

### `create_scram_user(cluster, admin_bootstrap, username, password)`
Create the SCRAM credential in cluster metadata. Run `kafka-configs.sh --alter --add-config
<scram_add_config> --entity-type users --entity-name <username>` with `cluster.run_tool(...)`. Use
`admin_bootstrap` (the trusted internal listener whose principal is a super user).

### `grant_topic_write(cluster, admin_bootstrap, principal, topic)`
Grant **produce-only** rights (`Write` and `Describe`) on exactly `topic` to `User:<principal>`. Use
`kafka-acls.sh --add --allow-principal ...`. Grant nothing broader (least privilege).

### `client_config(sasl_bootstrap, username, password) -> dict`
Return a producer config dict that authenticates over the SASL/SCRAM listener:
`bootstrap.servers`, `security.protocol="SASL_PLAINTEXT"`, `sasl.mechanism="SCRAM-SHA-256"`,
`sasl.username`, `sasl.password`.

## How it is graded

- **Pure config** (no broker): `scram_add_config` returns the exact string. `client_config` has the
  right SASL keys.
- **Live security** (native broker + authorizer, no Docker): the grader creates `app` and `evil`. It
  grants `app` the produce ACL on a topic. Then it asserts these three points:
  - `app` produces successfully.
  - `evil` (no ACL) is **denied** with an authorization failure.
  - The grant of `app` on topic A does **not** let it write topic B (the ACL is scoped).

> Note: the lab uses `SASL_PLAINTEXT` to stay free of certificates. In production, you would use
> `SASL_SSL` so the channel is encrypted — see the lesson.
