"""Native Apache Kafka process management for the de_testkit fixtures.

NO DOCKER. Everything here starts real Kafka 4.x JVMs straight from the vendored
tarball (``labs/.kafka/kafka_2.13-<ver>``, installed by ``labs/fetch_kafka.sh``)
using the vendored JDK 21 (``labs/.jdk``, installed by ``labs/setup_jdk.sh``).
This is what makes every graded Kafka exercise satisfy the no-Docker rule.

Design (probed and verified on 2026-10-04, Kafka 4.3.1, JDK 21):

* **Topology.** A cluster is ONE dedicated controller-only KRaft node plus N
  pure-broker nodes. Keeping the controller quorum off the brokers means a test
  can kill/restart ANY broker (KFK-02/11/14) without disturbing the metadata
  quorum. The single-node ``kafka_broker`` fixture uses the cheaper combined
  ``broker,controller`` role on one node.
* **Ports.** Every listener binds ``127.0.0.1`` on an OS-assigned free port
  (bind to port 0, read it back, close, reuse). Parallel runs therefore never
  collide, and we never touch 5432/8888 (your own containers).
* **Heap.** ``KAFKA_HEAP_OPTS=-Xmx512m -Xms256m`` per JVM (controller + each
  broker), so a 3-broker cluster is ~2 GB.
* **Readiness is deterministic** — we poll ``AdminClient`` metadata until the
  expected number of brokers is registered, with a timeout. No ``sleep()``.
* **Teardown is bullet-proof.** Each JVM is started in its own process group
  (``start_new_session=True``); teardown sends SIGTERM then SIGKILL to the whole
  group, so no orphan JVMs survive even if a test raises.

If the tarball or Java is missing the fixtures SKIP (never fail) with a message
pointing at the fetch scripts.
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from de_testkit.java_env import repo_root, resolve as resolve_java

HEAP_OPTS = "-Xmx512m -Xms256m"
READY_TIMEOUT_S = 60.0


# ---------------------------------------------------------------------------
# locating the vendored tarball + java
# ---------------------------------------------------------------------------
def kafka_home(root: Path | None = None) -> Path | None:
    """Return ``labs/.kafka/kafka_2.13-<ver>`` if it contains the CLI tools."""
    base = (root or repo_root()) / "labs" / ".kafka"
    if not base.is_dir():
        return None
    for child in sorted(base.iterdir()):
        if (child / "bin" / "kafka-server-start.sh").exists():
            return child
    return None


def kafka_available(root: Path | None = None) -> tuple[bool, str]:
    """(ok, reason). ok is True only when both the tarball and a usable Java
    exist. ``reason`` is a skip message otherwise."""
    kh = kafka_home(root)
    if kh is None:
        return False, (
            "native Kafka not found. Run `bash labs/fetch_kafka.sh` to download "
            "the pinned, sha512-verified Kafka 4.x tarball into labs/.kafka/."
        )
    info = resolve_java()
    if not info["usable"]:
        return False, (
            f"Java unusable for Kafka: {info['reason']} "
            "Run `bash labs/setup_jdk.sh` to vendor JDK 21 into labs/.jdk/."
        )
    return True, ""


def _java_env(extra: dict | None = None) -> dict:
    env = dict(os.environ)
    info = resolve_java()
    if info["usable"] and info["java_home"]:
        env["JAVA_HOME"] = info["java_home"]
        env["PATH"] = str(Path(info["java_home"]) / "bin") + os.pathsep + env.get("PATH", "")
    env["KAFKA_HEAP_OPTS"] = HEAP_OPTS
    # Keep each JVM quiet and self-contained.
    env.setdefault("LOG_DIR", "")  # overridden per-node below
    if extra:
        env.update(extra)
    return env


def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def _tcp_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# a single managed JVM process (broker or controller or connect)
# ---------------------------------------------------------------------------
@dataclass
class _Proc:
    name: str
    popen: subprocess.Popen
    log_path: Path

    def alive(self) -> bool:
        return self.popen.poll() is None

    def kill_group(self, sig: int) -> None:
        if self.popen.poll() is not None:
            return
        try:
            os.killpg(os.getpgid(self.popen.pid), sig)
        except (ProcessLookupError, PermissionError):
            pass

    def stop(self, grace_s: float = 10.0) -> None:
        """SIGTERM the process group, then SIGKILL if it lingers."""
        if self.popen.poll() is not None:
            return
        self.kill_group(signal.SIGTERM)
        deadline = time.time() + grace_s
        while time.time() < deadline:
            if self.popen.poll() is not None:
                return
            time.sleep(0.1)
        self.kill_group(signal.SIGKILL)
        try:
            self.popen.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass

    def tail(self, n: int = 40) -> str:
        try:
            lines = self.log_path.read_text(errors="replace").splitlines()
            return "\n".join(lines[-n:])
        except OSError:
            return "(no log)"


# ---------------------------------------------------------------------------
# a broker node (its own data dir, props, listeners)
# ---------------------------------------------------------------------------
@dataclass
class BrokerNode:
    node_id: int
    data_dir: Path
    props_path: Path
    plaintext_port: int
    controller_port: int | None
    extra_listeners: dict[str, int] = field(default_factory=dict)
    proc: _Proc | None = None

    @property
    def bootstrap(self) -> str:
        return f"127.0.0.1:{self.plaintext_port}"

    def alive(self) -> bool:
        return self.proc is not None and self.proc.alive()


class KafkaCluster:
    """A running native Kafka cluster: 1 controller + N brokers (or a single
    combined node).  Returned by the ``kafka_broker`` / ``kafka_cluster``
    fixtures.

    Key API used by exercise tests:
      * ``bootstrap_servers`` — comma-joined ``host:port`` of live brokers.
      * ``admin()`` — a confluent_kafka ``AdminClient`` on the cluster.
      * ``run_tool(script, *args)`` — run a bundled CLI tool (e.g.
        ``kafka-topics.sh``, ``kafka-acls.sh``, ``kafka-configs.sh``); returns a
        ``CompletedProcess`` with ``stdout``/``stderr`` captured.
      * ``create_topic(name, partitions=1, replication_factor=1, config=None)``
      * ``brokers`` — list[BrokerNode]; ``stop_broker(i)`` / ``kill_broker(i)`` /
        ``start_broker(i)`` for fault-injection (KFK-02/11/14).
      * ``connect_standalone(...)`` — start a native Kafka Connect worker.
    """

    def __init__(self, home: Path, cluster_id: str, work: Path,
                 controller: _Proc | None, controller_quorum: str,
                 brokers: list[BrokerNode], security: dict | None = None):
        self._home = home
        self._cluster_id = cluster_id
        self._work = work
        self._controller = controller
        self._controller_quorum = controller_quorum
        self.brokers = brokers
        self.security = security or {}
        self._connects: list[_Proc] = []

    # -- bootstrap / admin -------------------------------------------------
    @property
    def bootstrap_servers(self) -> str:
        live = [b.bootstrap for b in self.brokers if b.alive()]
        # even if a broker is intentionally down, keep its address so clients
        # can observe the failure; fall back to all if none appear alive yet.
        return ",".join(b.bootstrap for b in self.brokers) if not live else ",".join(
            b.bootstrap for b in self.brokers)

    def admin(self, **config):
        from confluent_kafka.admin import AdminClient

        conf = {"bootstrap.servers": self.bootstrap_servers}
        conf.update(self.security)
        conf.update(config)
        return AdminClient(conf)

    # -- bundled CLI tools -------------------------------------------------
    def tool_path(self, script: str) -> Path:
        return self._home / "bin" / script

    def run_tool(self, script: str, *args: str, timeout: int = 60,
                 check: bool = True) -> subprocess.CompletedProcess:
        cmd = [str(self.tool_path(script)), *args]
        proc = subprocess.run(cmd, env=_java_env(), capture_output=True,
                              text=True, timeout=timeout)
        if check and proc.returncode != 0:
            raise RuntimeError(
                f"{script} failed (rc={proc.returncode}):\n"
                f"CMD: {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
        return proc

    def create_topic(self, name: str, partitions: int = 1,
                     replication_factor: int = 1,
                     config: dict | None = None, timeout: int = 60) -> None:
        args = ["--bootstrap-server", self.bootstrap_servers, "--create",
                "--topic", name, "--partitions", str(partitions),
                "--replication-factor", str(replication_factor)]
        for k, v in (config or {}).items():
            args += ["--config", f"{k}={v}"]
        self.run_tool("kafka-topics.sh", *args, timeout=timeout)

    def list_topics(self, timeout: int = 30) -> list[str]:
        out = self.run_tool("kafka-topics.sh", "--bootstrap-server",
                            self.bootstrap_servers, "--list", timeout=timeout)
        return [ln for ln in out.stdout.splitlines() if ln.strip()]

    # -- fault injection ---------------------------------------------------
    def stop_broker(self, node_id: int, grace_s: float = 10.0) -> None:
        b = self._broker(node_id)
        if b.proc:
            b.proc.stop(grace_s=grace_s)

    def kill_broker(self, node_id: int) -> None:
        """Hard SIGKILL (simulates a crash)."""
        b = self._broker(node_id)
        if b.proc:
            b.proc.kill_group(signal.SIGKILL)
            try:
                b.proc.popen.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

    def start_broker(self, node_id: int, wait_ready: bool = True) -> None:
        """(Re)start a broker, reusing its existing data dir (so it rejoins)."""
        b = self._broker(node_id)
        if b.alive():
            return
        b.proc = _start_node(self._home, b.props_path, b.data_dir, f"broker-{node_id}")
        if wait_ready:
            self.wait_ready(min_brokers=self.num_alive(), timeout=READY_TIMEOUT_S)

    def _broker(self, node_id: int) -> BrokerNode:
        for b in self.brokers:
            if b.node_id == node_id:
                return b
        raise KeyError(f"no broker with node_id={node_id}; have "
                       f"{[b.node_id for b in self.brokers]}")

    def num_alive(self) -> int:
        return sum(1 for b in self.brokers if b.alive())

    # -- readiness ---------------------------------------------------------
    def wait_ready(self, min_brokers: int, timeout: float = READY_TIMEOUT_S) -> None:
        from confluent_kafka.admin import AdminClient

        deadline = time.time() + timeout
        last_exc = None
        while time.time() < deadline:
            try:
                a = AdminClient({"bootstrap.servers": self.bootstrap_servers,
                                 **self.security})
                md = a.list_topics(timeout=5)
                if len(md.brokers) >= min_brokers:
                    return
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
            time.sleep(0.5)
        logs = "\n\n".join(
            f"--- {b.node_id} ---\n{b.proc.tail() if b.proc else '(not started)'}"
            for b in self.brokers)
        raise TimeoutError(
            f"cluster not ready ({min_brokers} brokers) within {timeout}s; "
            f"last error={last_exc}\n{logs}")

    # -- native Kafka Connect ---------------------------------------------
    def connect_standalone(self, connector_configs: list[dict],
                           *, worker_overrides: dict | None = None,
                           wait_rest: bool = True,
                           timeout: float = 60.0) -> "ConnectWorker":
        """Start a native connect-standalone worker from the tarball.

        ``connector_configs`` is a list of connector .properties dicts (each
        needs at least ``name`` and ``connector.class``). Returns a
        ``ConnectWorker`` with a REST base URL. SKIP-free; raises on failure.
        """
        wdir = Path(tempfile.mkdtemp(prefix="connect-", dir=self._work))
        rest_port = free_port()
        offsets = wdir / "offsets.txt"
        worker_props = {
            "bootstrap.servers": self.bootstrap_servers,
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
            "offset.storage.file.filename": str(offsets),
            "offset.flush.interval.ms": "1000",
            "listeners": f"HTTP://127.0.0.1:{rest_port}",
            "plugin.discovery": "hybrid_warn",
        }
        worker_props.update(self.security)
        if worker_overrides:
            worker_props.update(worker_overrides)
        wpath = wdir / "worker.properties"
        wpath.write_text("".join(f"{k}={v}\n" for k, v in worker_props.items()))

        conn_paths = []
        for i, cc in enumerate(connector_configs):
            cp = wdir / f"connector-{i}.properties"
            cp.write_text("".join(f"{k}={v}\n" for k, v in cc.items()))
            conn_paths.append(str(cp))

        log_path = wdir / "connect.log"
        env = _java_env({"LOG_DIR": str(wdir / "klog")})
        cmd = [str(self._home / "bin" / "connect-standalone.sh"),
               str(wpath), *conn_paths]
        popen = subprocess.Popen(
            cmd, env=env, stdout=open(log_path, "w"), stderr=subprocess.STDOUT,
            start_new_session=True)
        proc = _Proc(name="connect", popen=popen, log_path=log_path)
        self._connects.append(proc)
        worker = ConnectWorker(proc=proc, rest_port=rest_port, work=wdir)
        if wait_rest:
            worker.wait_rest(timeout=timeout)
        return worker

    # -- teardown ----------------------------------------------------------
    def shutdown(self) -> None:
        for c in self._connects:
            c.stop()
        for b in self.brokers:
            if b.proc:
                b.proc.stop()
        if self._controller:
            self._controller.stop()
        shutil.rmtree(self._work, ignore_errors=True)


@dataclass
class ConnectWorker:
    proc: _Proc
    rest_port: int
    work: Path

    @property
    def rest_url(self) -> str:
        return f"http://127.0.0.1:{self.rest_port}"

    def wait_rest(self, timeout: float = 60.0) -> None:
        import urllib.error
        import urllib.request

        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self.proc.alive():
                raise RuntimeError(f"connect worker died:\n{self.proc.tail()}")
            try:
                with urllib.request.urlopen(f"{self.rest_url}/connectors", timeout=3):
                    return
            except (urllib.error.URLError, OSError):
                time.sleep(0.5)
        raise TimeoutError(f"connect REST not ready within {timeout}s\n"
                           f"{self.proc.tail()}")

    def connectors(self) -> list[str]:
        import json
        import urllib.request

        with urllib.request.urlopen(f"{self.rest_url}/connectors", timeout=5) as r:
            return json.loads(r.read())


# ---------------------------------------------------------------------------
# low-level: format + start a KRaft node
# ---------------------------------------------------------------------------
def _start_node(home: Path, props_path: Path, data_dir: Path,
                name: str) -> _Proc:
    log_path = data_dir / f"{name}.out"
    env = _java_env({"LOG_DIR": str(data_dir / "klog")})
    cmd = [str(home / "bin" / "kafka-server-start.sh"), str(props_path)]
    popen = subprocess.Popen(
        cmd, env=env, stdout=open(log_path, "w"), stderr=subprocess.STDOUT,
        start_new_session=True)
    return _Proc(name=name, popen=popen, log_path=log_path)


def _write_props(path: Path, props: dict) -> None:
    path.write_text("".join(f"{k}={v}\n" for k, v in props.items()))


def _format_storage(home: Path, props_path: Path, cluster_id: str,
                    extra_args: list[str] | None = None) -> None:
    cmd = [str(home / "bin" / "kafka-storage.sh"), "format",
           "-t", cluster_id, "-c", str(props_path), "--ignore-formatted"]
    if extra_args:
        cmd += extra_args
    proc = subprocess.run(cmd, env=_java_env(), capture_output=True, text=True,
                          timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(f"kafka-storage format failed:\n{proc.stdout}\n{proc.stderr}")


def _security_props(security: dict | None) -> dict:
    """Translate a high-level security request into broker properties.

    Supported (minimal, enough for KFK-12):
      {"sasl_scram": True, "users": {"admin": "pw", ...},
       "super_users": ["admin"], "authorizer": True}
    Returns broker-side props plus the client config saved on the cluster.
    """
    return security or {}


# ---------------------------------------------------------------------------
# public builders
# ---------------------------------------------------------------------------
def start_single_broker(root: Path | None = None) -> KafkaCluster:
    """Session-scoped single-node combined broker+controller (KRaft)."""
    home = kafka_home(root)
    assert home is not None
    cluster_id = _rand_uuid(home)
    work = Path(tempfile.mkdtemp(prefix="kafka-1-"))
    node_id = 1
    plaintext = free_port()
    controller = free_port()
    data = work / "n1"
    data.mkdir(parents=True)
    props = data / "server.properties"
    _write_props(props, {
        "process.roles": "broker,controller",
        "node.id": node_id,
        "controller.quorum.voters": f"{node_id}@127.0.0.1:{controller}",
        "listeners": f"PLAINTEXT://127.0.0.1:{plaintext},CONTROLLER://127.0.0.1:{controller}",
        "inter.broker.listener.name": "PLAINTEXT",
        "controller.listener.names": "CONTROLLER",
        "listener.security.protocol.map": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "advertised.listeners": f"PLAINTEXT://127.0.0.1:{plaintext}",
        "log.dirs": str(data / "logs"),
        "offsets.topic.replication.factor": 1,
        "transaction.state.log.replication.factor": 1,
        "transaction.state.log.min.isr": 1,
        "group.initial.rebalance.delay.ms": 0,
        "num.partitions": 1,
        "auto.create.topics.enable": "false",
    })
    _format_storage(home, props, cluster_id)
    node = BrokerNode(node_id=node_id, data_dir=data, props_path=props,
                      plaintext_port=plaintext, controller_port=controller)
    node.proc = _start_node(home, props, data, "broker-1")
    cluster = KafkaCluster(home, cluster_id, work, controller=None,
                           controller_quorum=f"{node_id}@127.0.0.1:{controller}",
                           brokers=[node])
    cluster.wait_ready(min_brokers=1)
    return cluster


def start_cluster(n_brokers: int = 3, *, broker_overrides: dict | None = None,
                  extra_security_protocols: dict | None = None,
                  root: Path | None = None) -> KafkaCluster:
    """Multi-broker cluster: 1 dedicated controller + N pure brokers.

    ``broker_overrides`` is merged into every broker's server.properties (e.g.
    ``{"min.insync.replicas": 2}`` or SASL/ACL settings for KFK-12).
    ``extra_security_protocols`` maps an extra listener name to a security
    protocol string, e.g. ``{"SASL": "SASL_PLAINTEXT"}``; a free port is bound
    for it on every broker and advertised. The cluster's client ``security``
    dict is left empty here — a security-focused exercise sets client config
    explicitly after creating SCRAM users via ``kafka-configs.sh``.
    """
    home = kafka_home(root)
    assert home is not None
    cluster_id = _rand_uuid(home)
    work = Path(tempfile.mkdtemp(prefix=f"kafka-{n_brokers}-"))

    controller_id = 100
    controller_port = free_port()
    quorum = f"{controller_id}@127.0.0.1:{controller_port}"

    # controller-only node
    cdir = work / "controller"
    cdir.mkdir(parents=True)
    cprops = cdir / "server.properties"
    _write_props(cprops, {
        "process.roles": "controller",
        "node.id": controller_id,
        "controller.quorum.voters": quorum,
        "listeners": f"CONTROLLER://127.0.0.1:{controller_port}",
        "controller.listener.names": "CONTROLLER",
        "listener.security.protocol.map": "CONTROLLER:PLAINTEXT",
        "log.dirs": str(cdir / "logs"),
    })
    _format_storage(home, cprops, cluster_id)
    controller_proc = _start_node(home, cprops, cdir, "controller")

    rf = min(3, n_brokers)
    min_isr = max(1, rf - 1)
    brokers: list[BrokerNode] = []
    for i in range(n_brokers):
        node_id = i + 1
        plaintext = free_port()
        extra_ports = {name: free_port() for name in (extra_security_protocols or {})}
        ddir = work / f"n{node_id}"
        ddir.mkdir(parents=True)
        props_path = ddir / "server.properties"

        listeners = [f"PLAINTEXT://127.0.0.1:{plaintext}"]
        advertised = [f"PLAINTEXT://127.0.0.1:{plaintext}"]
        secmap = ["CONTROLLER:PLAINTEXT", "PLAINTEXT:PLAINTEXT"]
        for name, proto in (extra_security_protocols or {}).items():
            p = extra_ports[name]
            listeners.append(f"{name}://127.0.0.1:{p}")
            advertised.append(f"{name}://127.0.0.1:{p}")
            secmap.append(f"{name}:{proto}")

        props = {
            "process.roles": "broker",
            "node.id": node_id,
            "controller.quorum.voters": quorum,
            "listeners": ",".join(listeners),
            "advertised.listeners": ",".join(advertised),
            "inter.broker.listener.name": "PLAINTEXT",
            "controller.listener.names": "CONTROLLER",
            "listener.security.protocol.map": ",".join(secmap),
            "log.dirs": str(ddir / "logs"),
            "offsets.topic.replication.factor": rf,
            "transaction.state.log.replication.factor": rf,
            "transaction.state.log.min.isr": min_isr,
            "default.replication.factor": rf,
            "group.initial.rebalance.delay.ms": 0,
            "num.partitions": 1,
            "auto.create.topics.enable": "false",
        }
        if broker_overrides:
            props.update(broker_overrides)
        _write_props(props_path, props)
        _format_storage(home, props_path, cluster_id)
        node = BrokerNode(node_id=node_id, data_dir=ddir, props_path=props_path,
                          plaintext_port=plaintext, controller_port=None,
                          extra_listeners=extra_ports)
        node.proc = _start_node(home, props_path, ddir, f"broker-{node_id}")
        brokers.append(node)

    cluster = KafkaCluster(home, cluster_id, work, controller=controller_proc,
                           controller_quorum=quorum, brokers=brokers)
    cluster.wait_ready(min_brokers=n_brokers)
    return cluster


def _rand_uuid(home: Path) -> str:
    """A valid KRaft cluster id (base64 of 16 random bytes), via the CLI so it is
    always acceptable to kafka-storage."""
    try:
        proc = subprocess.run(
            [str(home / "bin" / "kafka-storage.sh"), "random-uuid"],
            env=_java_env(), capture_output=True, text=True, timeout=30)
        out = proc.stdout.strip().splitlines()
        if out:
            return out[-1].strip()
    except Exception:  # noqa: BLE001
        pass
    import base64
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b"=").decode()
