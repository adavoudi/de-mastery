"""Tests for KFK-12 / 01-lockdown.

Pure config layer (no broker -> G1 reachable on a bare machine):
  * scram_add_config and client_config build the exact strings/dict the CLI and
    client expect.
Live security layer (native, no Docker):
  * build a combined broker+controller node with StandardAuthorizer + a
    SASL/SCRAM listener, use the learner's functions to create users + grant a
    least-privilege ACL, then assert the allowed principal produces and the
    disallowed principal is DENIED (TOPIC_AUTHORIZATION_FAILED).

The secured node is built with the de_testkit kafka_support helpers (the plain
kafka_cluster factory uses a dedicated controller that does not carry the
authorizer, so ACL management needs a combined-role node). The whole live layer
SKIPS cleanly if the Kafka tarball / JDK are not vendored.
"""

import tempfile
import uuid
from pathlib import Path

import pytest

from de_testkit import kafka_support as ks


def _uid():
    return uuid.uuid4().hex[:8]


# --------------------------------------------------------------------------
# pure config layer
# --------------------------------------------------------------------------
def test_scram_add_config(submission):
    assert submission.scram_add_config("app-pw") == "SCRAM-SHA-256=[password=app-pw]"


def test_client_config_shape(submission):
    cfg = submission.client_config("127.0.0.1:9", "app", "app-pw")
    assert cfg["bootstrap.servers"] == "127.0.0.1:9"
    assert cfg["security.protocol"] == "SASL_PLAINTEXT"
    assert cfg["sasl.mechanism"] == "SCRAM-SHA-256"
    assert cfg["sasl.username"] == "app"
    assert cfg["sasl.password"] == "app-pw"


# --------------------------------------------------------------------------
# secured-cluster fixture (combined broker+controller with authorizer + SASL)
# --------------------------------------------------------------------------
@pytest.fixture
def secured_cluster():
    ok, reason = ks.kafka_available()
    if not ok:
        pytest.skip(reason)

    home = ks.kafka_home()
    cluster_id = ks._rand_uuid(home)
    work = Path(tempfile.mkdtemp(prefix="kafka-sec-"))
    data = work / "n1"
    data.mkdir(parents=True)
    plaintext = ks.free_port()
    controller = ks.free_port()
    sasl = ks.free_port()
    props = data / "server.properties"
    ks._write_props(props, {
        "process.roles": "broker,controller",
        "node.id": 1,
        "controller.quorum.voters": f"1@127.0.0.1:{controller}",
        "listeners": (f"PLAINTEXT://127.0.0.1:{plaintext},"
                      f"CONTROLLER://127.0.0.1:{controller},"
                      f"SASL://127.0.0.1:{sasl}"),
        "inter.broker.listener.name": "PLAINTEXT",
        "controller.listener.names": "CONTROLLER",
        "listener.security.protocol.map":
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL:SASL_PLAINTEXT",
        "advertised.listeners":
            f"PLAINTEXT://127.0.0.1:{plaintext},SASL://127.0.0.1:{sasl}",
        "log.dirs": str(data / "logs"),
        "offsets.topic.replication.factor": 1,
        "transaction.state.log.replication.factor": 1,
        "transaction.state.log.min.isr": 1,
        "group.initial.rebalance.delay.ms": 0,
        "num.partitions": 1,
        "auto.create.topics.enable": "false",
        # security: authenticate via SCRAM, authorize via StandardAuthorizer,
        # trust the internal PLAINTEXT listener (ANONYMOUS) as super user so
        # cluster-management traffic (and our ACL/user setup) is not blocked.
        "authorizer.class.name":
            "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
        "super.users": "User:ANONYMOUS",
        "sasl.enabled.mechanisms": "SCRAM-SHA-256",
        "listener.name.sasl.scram-sha-256.sasl.jaas.config":
            "org.apache.kafka.common.security.scram.ScramLoginModule required;",
    })
    ks._format_storage(home, props, cluster_id)
    node = ks.BrokerNode(node_id=1, data_dir=data, props_path=props,
                         plaintext_port=plaintext, controller_port=controller,
                         extra_listeners={"SASL": sasl})
    node.proc = ks._start_node(home, props, data, "broker-1")
    cluster = ks.KafkaCluster(home, cluster_id, work, controller=None,
                              controller_quorum=f"1@127.0.0.1:{controller}",
                              brokers=[node])
    try:
        cluster.wait_ready(min_brokers=1)
        cluster.sasl_bootstrap = f"127.0.0.1:{sasl}"
        yield cluster
    finally:
        cluster.shutdown()


def _produce_error(submission, sasl_bs, user, pw, topic):
    """Produce one record as `user`; return the delivery error (None if OK)."""
    from confluent_kafka import Producer

    cfg = submission.client_config(sasl_bs, user, pw)
    cfg["message.timeout.ms"] = 8000
    p = Producer(cfg)
    errors = []
    p.produce(topic, value=b"x", on_delivery=lambda e, m: errors.append(e))
    p.flush(10)
    return errors[0] if errors else None


def test_allowed_produces_denied_blocked(secured_cluster, submission):
    c = secured_cluster
    admin_bs = c.bootstrap_servers          # trusted PLAINTEXT (super user)
    sasl_bs = c.sasl_bootstrap
    topic = f"orders-{_uid()}"

    # create both principals' SCRAM credentials
    submission.create_scram_user(c, admin_bs, "app", "app-pw")
    submission.create_scram_user(c, admin_bs, "evil", "evil-pw")
    c.create_topic(topic, partitions=1, replication_factor=1)

    # grant ONLY app a produce ACL
    submission.grant_topic_write(c, admin_bs, "app", topic)

    # allowed principal succeeds
    err_app = _produce_error(submission, sasl_bs, "app", "app-pw", topic)
    assert err_app is None, f"allowed principal was blocked: {err_app}"

    # disallowed principal is denied
    err_evil = _produce_error(submission, sasl_bs, "evil", "evil-pw", topic)
    assert err_evil is not None, "disallowed principal was NOT denied"
    assert "AUTHORIZATION" in str(err_evil).upper(), (
        f"expected an authorization failure, got {err_evil}")


def test_acl_scoped_to_topic(secured_cluster, submission):
    """app's ACL on topic A must not let it write topic B (least privilege)."""
    c = secured_cluster
    admin_bs = c.bootstrap_servers
    sasl_bs = c.sasl_bootstrap
    topic_a = f"a-{_uid()}"
    topic_b = f"b-{_uid()}"

    submission.create_scram_user(c, admin_bs, "app", "app-pw")
    c.create_topic(topic_a, partitions=1, replication_factor=1)
    c.create_topic(topic_b, partitions=1, replication_factor=1)
    submission.grant_topic_write(c, admin_bs, "app", topic_a)

    assert _produce_error(submission, sasl_bs, "app", "app-pw", topic_a) is None
    err_b = _produce_error(submission, sasl_bs, "app", "app-pw", topic_b)
    assert err_b is not None and "AUTHORIZATION" in str(err_b).upper()
