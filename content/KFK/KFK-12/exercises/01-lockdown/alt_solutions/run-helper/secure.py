"""Alt solution 2 — str.format for the config value, a small run-helper that
captures the admin bootstrap once, and client_config built by mutating a dict."""

from __future__ import annotations

SCRAM_MECHANISM = "SCRAM-SHA-256"


def scram_add_config(password):
    return "{mech}=[password={pw}]".format(mech=SCRAM_MECHANISM, pw=password)


def _admin(cluster, admin_bootstrap):
    def run(script, *rest):
        return cluster.run_tool(script, "--bootstrap-server", admin_bootstrap, *rest)
    return run


def create_scram_user(cluster, admin_bootstrap, username, password):
    run = _admin(cluster, admin_bootstrap)
    run("kafka-configs.sh", "--alter", "--add-config", scram_add_config(password),
        "--entity-type", "users", "--entity-name", username)


def grant_topic_write(cluster, admin_bootstrap, principal, topic):
    run = _admin(cluster, admin_bootstrap)
    run("kafka-acls.sh", "--add", "--allow-principal", f"User:{principal}",
        "--operation", "Write", "--operation", "Describe", "--topic", topic)


def client_config(sasl_bootstrap, username, password):
    cfg = {}
    cfg["bootstrap.servers"] = sasl_bootstrap
    cfg["security.protocol"] = "SASL_PLAINTEXT"
    cfg["sasl.mechanism"] = SCRAM_MECHANISM
    cfg["sasl.username"] = username
    cfg["sasl.password"] = password
    return cfg
