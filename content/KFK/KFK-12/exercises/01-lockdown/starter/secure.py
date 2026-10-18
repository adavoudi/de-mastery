"""Starter — KFK-12. Implement the SASL/SCRAM + ACL functions. The stubs below
currently fail the tests (gate G1)."""

from __future__ import annotations

SCRAM_MECHANISM = "SCRAM-SHA-256"


def scram_add_config(password):
    """Return the --add-config value for a SCRAM user, e.g.
    'SCRAM-SHA-256=[password=secret]'."""
    # TODO
    return ""


def create_scram_user(cluster, admin_bootstrap, username, password):
    """Create a SCRAM credential via kafka-configs.sh (use cluster.run_tool)."""
    # TODO
    pass


def grant_topic_write(cluster, admin_bootstrap, principal, topic):
    """Grant Write + Describe on `topic` to User:<principal> via kafka-acls.sh."""
    # TODO
    pass


def client_config(sasl_bootstrap, username, password):
    """Return a producer config dict authenticating as `username` over SASL/SCRAM."""
    # TODO
    return {}
