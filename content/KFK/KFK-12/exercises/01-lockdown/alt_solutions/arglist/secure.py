"""Alt solution 1 — build the CLI argument lists incrementally and the client
config via a dict comprehension; operations granted in a different order. Same
observable behaviour (produce-only on exactly the topic)."""

from __future__ import annotations

SCRAM_MECHANISM = "SCRAM-SHA-256"


def scram_add_config(password):
    return "%s=[password=%s]" % (SCRAM_MECHANISM, password)


def create_scram_user(cluster, admin_bootstrap, username, password):
    args = ["--bootstrap-server", admin_bootstrap, "--alter"]
    args += ["--add-config", scram_add_config(password)]
    args += ["--entity-type", "users", "--entity-name", username]
    cluster.run_tool("kafka-configs.sh", *args)


def grant_topic_write(cluster, admin_bootstrap, principal, topic):
    args = ["--bootstrap-server", admin_bootstrap, "--add"]
    args += ["--allow-principal", f"User:{principal}"]
    for op in ("Describe", "Write"):      # order does not matter to the authorizer
        args += ["--operation", op]
    args += ["--topic", topic]
    cluster.run_tool("kafka-acls.sh", *args)


def client_config(sasl_bootstrap, username, password):
    base = {
        "bootstrap.servers": sasl_bootstrap,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": SCRAM_MECHANISM,
    }
    creds = {"sasl.username": username, "sasl.password": password}
    return {**base, **creds}
