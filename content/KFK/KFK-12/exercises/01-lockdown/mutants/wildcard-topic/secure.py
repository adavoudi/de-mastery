"""Reference solution — KFK-12 SASL/SCRAM + ACL lockdown.

Pure config builders plus thin wrappers over the bundled CLI tools. The grader
builds a SASL/SCRAM + authorizer cluster, calls these to create users and grant a
least-privilege ACL, then asserts an allowed principal produces and a disallowed
one is denied.
"""

from __future__ import annotations

SCRAM_MECHANISM = "SCRAM-SHA-256"


def scram_add_config(password):
    """The value kafka-configs.sh expects after --add-config for a SCRAM user."""
    return f"{SCRAM_MECHANISM}=[password={password}]"


def create_scram_user(cluster, admin_bootstrap, username, password):
    """Create a SCRAM credential in the cluster metadata via kafka-configs.sh.

    ``admin_bootstrap`` is the trusted (super-user) PLAINTEXT listener.
    """
    cluster.run_tool(
        "kafka-configs.sh", "--bootstrap-server", admin_bootstrap, "--alter",
        "--add-config", scram_add_config(password),
        "--entity-type", "users", "--entity-name", username,
    )


def grant_topic_write(cluster, admin_bootstrap, principal, topic):
    """Grant produce-only (Write + Describe) on ``topic`` to ``User:principal``."""
    cluster.run_tool(
        "kafka-acls.sh", "--bootstrap-server", admin_bootstrap, "--add",
        "--allow-principal", f"User:{principal}",
        "--operation", "Write", "--operation", "Describe",
        "--topic", "*",  # BUG: wildcard grant — not scoped to the one topic
    )


def client_config(sasl_bootstrap, username, password):
    """Build a producer config dict that authenticates as ``username`` over the
    SASL/SCRAM listener."""
    return {
        "bootstrap.servers": sasl_bootstrap,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": SCRAM_MECHANISM,
        "sasl.username": username,
        "sasl.password": password,
    }
