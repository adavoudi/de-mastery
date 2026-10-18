#!/usr/bin/env python3
"""Regenerate broker_alt.py from broker_full.py (genuinely different IO + read).
Keeps the protocol/storage logic; swaps asyncio for a threaded socketserver and
the index-seek read for a full scan.  Run from byo/mini-kafka/_src/.
"""
from pathlib import Path

HERE = Path(__file__).parent
full = (HERE / "broker_full.py").read_text()

# 1. serve() + _handle() -> threaded socketserver serve()
old_serve = '''    async def serve(self):
        server = await asyncio.start_server(self._handle, self.host, self.port)
        self.port = server.sockets[0].getsockname()[1]
        print(f"LISTENING {self.host}:{self.port}", flush=True)
        async with server:
            await server.serve_forever()

    async def _handle(self, reader, writer):
        try:
            while True:
                size_bytes = await reader.readexactly(4)
                (size,) = struct.unpack(">i", size_bytes)
                payload = await reader.readexactly(size)
                resp = self.dispatch(payload)
                writer.write(struct.pack(">i", len(resp)) + resp)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    def dispatch(self, payload):'''

new_serve = '''    def serve(self):
        import socketserver
        import threading
        broker = self
        broker._lock = threading.Lock()

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                sock = self.request
                while True:
                    size_bytes = _recv_n(sock, 4)
                    if not size_bytes:
                        return
                    (size,) = struct.unpack(">i", size_bytes)
                    payload = _recv_n(sock, size)
                    if payload is None:
                        return
                    with broker._lock:
                        resp = broker.dispatch(payload)
                    sock.sendall(struct.pack(">i", len(resp)) + resp)

        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True

        srv = Server((self.host, self.port), Handler)
        self.port = srv.socket.getsockname()[1]
        print(f"LISTENING {self.host}:{self.port}", flush=True)
        srv.serve_forever()

    def dispatch(self, payload):'''

assert old_serve in full, "serve block not found"
alt = full.replace(old_serve, new_serve)

# 2. storage read -> full scan
old_read = '''    def read(self, fetch_offset, max_bytes):
        if fetch_offset >= self.high_watermark:
            return b""
        seg = self.segments[0]
        for s in self.segments:
            if s.base_offset <= fetch_offset:
                seg = s
            else:
                break
        start_pos = 0
        for rel, pos in seg.index_entries():
            if seg.base_offset + rel <= fetch_offset:
                start_pos = pos
            else:
                break
        collected = io.BytesIO()
        collected.write(seg.read_from(start_pos))
        for s in self.segments[self.segments.index(seg) + 1:]:
            collected.write(s.read_from(0))
        out = io.BytesIO(); total = 0'''

new_read = '''    def read(self, fetch_offset, max_bytes):
        if fetch_offset >= self.high_watermark:
            return b""
        # ALT: concatenate every segment and linearly scan (correct for the
        # small logs tests use) instead of using the index to jump.
        collected = io.BytesIO()
        for s in self.segments:
            collected.write(s.read_from(0))
        out = io.BytesIO(); total = 0'''

assert old_read in alt, "read block not found"
alt = alt.replace(old_read, new_read)

# 3. main() -> call serve() directly + add _recv_n helper
old_main = '''def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--node-id", type=int, default=1)
    args = ap.parse_args(argv)
    broker = Broker(args.host, args.port, Path(args.data_dir), args.node_id)
    try:
        asyncio.run(broker.serve())
    except KeyboardInterrupt:
        pass'''

new_main = '''def _recv_n(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None if not buf else buf
        buf += chunk
    return buf


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--node-id", type=int, default=1)
    args = ap.parse_args(argv)
    broker = Broker(args.host, args.port, Path(args.data_dir), args.node_id)
    try:
        broker.serve()
    except KeyboardInterrupt:
        pass'''

assert old_main in alt, "main block not found"
alt = alt.replace(old_main, new_main)

alt = alt.replace(
    '"""Mini Kafka — a from-scratch broker',
    '"""Mini Kafka (ALT) — threaded socketserver + full-scan reads.\n\nA from-scratch broker')

(HERE / "broker_alt.py").write_text(alt)
print("regenerated broker_alt.py")
