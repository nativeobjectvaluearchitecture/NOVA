"""SchemaAgent: user-defined message boilerplate on top of SchemaRegistry.

This module is the "bring your own schema" convenience layer discussed in
the design conversation: NOVA does not force any particular field layout
(sender/receiver/type/payload is just an example, see `DEFAULT_SCHEMA`).
Instead, the *user* defines a `Schema` -- an ordered tuple of field names
meaning whatever they want -- and `SchemaAgent` hides everything else:

    user-defined schema
            |
            v
      SchemaAgent (this module)
            |
    +-------+--------+
    |                |
 encode/decode    socket + framing
 (schema_registry)   (transport.py)
    |                |
    +-------+--------+
            |
            v
        the wire

What the user writes
---------------------
    from nova import Schema
    from nova.sync.schema_agent import SchemaAgent

    MY_SCHEMA = Schema("MY-PROTOCOL", 1, ("sender", "receiver", "type", "payload"))

    server = SchemaAgent(MY_SCHEMA, secret=b"shared-secret")
    server.listen("127.0.0.1", 9000)

    for msg in server.receive_forever():
        print(msg["sender"], msg["payload"])

    # elsewhere
    client = SchemaAgent(MY_SCHEMA, secret=b"shared-secret")
    client.connect("127.0.0.1", 9000)
    client.send(sender="analyst", receiver="build", type=2, payload={"project": "SSPC"})

No socket handling, framing, signing, replay/staleness checks, or codec
selection is visible to the user -- NOVA owns all of that. The user only
owns the *meaning* of the positions in their schema.
"""

from __future__ import annotations

import socket
from typing import Any, Dict, Iterator, Optional

from ..schema_registry import Schema, SchemaRegistry
from .transport import send_frame, recv_frame


class SchemaAgent:
    """Minimal request-less, connection-oriented pipe for user-defined schemas.

    This is intentionally simpler than `AgentClient`/`AgentServer` (no
    method dispatch, no request/response correlation) -- it's meant for
    the "define your own message shape and just move it" use case, e.g.
    fixed pipelines between a small number of long-lived agents.
    """

    def __init__(
        self,
        schema: Schema,
        secret: bytes,
        registry: Optional[SchemaRegistry] = None,
    ):
        self.schema = schema
        self.secret = secret
        self.registry = registry or SchemaRegistry()
        self.registry.register(schema)
        self._server_sock: Optional[socket.socket] = None
        self._conn: Optional[socket.socket] = None

    # -- Client side -----------------------------------------------------

    def connect(self, host: str, port: int, timeout: float = 30.0) -> None:
        """Open a persistent connection to a listening SchemaAgent."""
        self._conn = socket.create_connection((host, port), timeout=timeout)

    def send(self, **values: Any) -> None:
        """Encode `values` (keyed by this schema's field names) and send them.

        Example: for schema fields ("sender", "receiver", "type", "payload"):
            agent.send(sender="analyst", receiver="build", type=2, payload={...})
        """
        if self._conn is None:
            raise RuntimeError("Not connected -- call connect() first")
        frame = self.registry.encode(
            self.schema, values, self.secret, sender=str(values.get("sender", "unknown"))
        )
        send_frame(self._conn, frame)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        if self._server_sock is not None:
            self._server_sock.close()
            self._server_sock = None

    # -- Server side -------------------------------------------------------

    def listen(self, host: str, port: int, backlog: int = 128) -> None:
        """Bind and listen for a single incoming connection.

        Kept simple on purpose: one persistent peer per SchemaAgent. For
        multi-client fan-in, run multiple SchemaAgent instances or use
        AgentServer instead.
        """
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((host, port))
        self._server_sock.listen(backlog)

    def accept(self, timeout: Optional[float] = None) -> None:
        """Block until a client connects, then hold that connection."""
        if self._server_sock is None:
            raise RuntimeError("Not listening -- call listen() first")
        if timeout is not None:
            self._server_sock.settimeout(timeout)
        conn, _addr = self._server_sock.accept()
        self._conn = conn

    def receive(self) -> Dict[str, Any]:
        """Receive and decode a single message as a dict keyed by schema field names.

        Includes bonus keys `_message_id`, `_sequence`, `_timestamp` for
        diagnostics/replay auditing -- ignore them if you don't need them.
        """
        if self._conn is None:
            raise RuntimeError("No active connection -- call accept() or connect() first")
        frame = recv_frame(self._conn)
        return self.registry.decode(frame, self.secret)

    def receive_forever(self) -> Iterator[Dict[str, Any]]:
        """Yield decoded messages until the connection closes."""
        while True:
            try:
                yield self.receive()
            except ConnectionError:
                return

    def __enter__(self) -> "SchemaAgent":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
