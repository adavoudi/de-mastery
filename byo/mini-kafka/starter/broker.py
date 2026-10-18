#!/usr/bin/env python3
"""Mini Kafka — YOUR broker. Build it up across the 8 stages.

This starter gives you the process scaffolding so the test harness can launch
you and talk to you, but NO protocol logic yet. Every stage's tests will fail
until you implement that stage.

What is already here:
  * argument parsing (--data-dir, --host, --port, --node-id),
  * an asyncio TCP server that length-frames requests (4-byte big-endian size
    prefix, same for responses),
  * ``LISTENING <host>:<port>`` printed on stdout once the socket is bound, so
    the harness knows you are ready without sleeping,
  * a ``dispatch(request_bytes) -> response_bytes`` method you must fill in.

Stage 1 (ApiVersions): parse the request header (api_key, api_version,
correlation_id, client_id) and return a correct ApiVersions response so a real
Kafka client / ``kafka-broker-api-versions.sh`` accepts it. Read the stage-01
README for the exact byte layout and the minimal API set to advertise.

Keep ONE file. Later stages extend ``dispatch`` and add storage; do not rename
``broker.py`` or the ``LISTENING`` line (the harness depends on both).
"""

from __future__ import annotations

import argparse
import asyncio
import struct
from pathlib import Path


class Broker:
    def __init__(self, host, port, data_dir, node_id=1):
        self.host = host
        self.port = port
        self.node_id = node_id
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    async def serve(self):
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

    def dispatch(self, request_bytes: bytes) -> bytes:
        """Parse a Kafka request and return the response body (without the
        4-byte length prefix — ``_handle`` adds that).

        TODO stage 1: implement at least ApiVersions (api_key 18). Right now we
        echo back only the correlation id, which no real client accepts, so the
        stage-1 tests fail (that is expected until you implement it).
        """
        # correlation id is the 3rd field of the request header (after api_key
        # int16 and api_version int16). This stub returns ONLY that, which is
        # not a valid body for any API.
        corr_id = struct.unpack_from(">i", request_bytes, 4)[0]
        return struct.pack(">i", corr_id)


def main(argv=None):
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
        pass


if __name__ == "__main__":
    main()
