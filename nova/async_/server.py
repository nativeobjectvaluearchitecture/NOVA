"""AsyncAgentServer: non-blocking counterpart to AgentServer.

Uses asyncio.start_server so one process can handle many concurrent agent
calls without a thread per connection. Registered methods may be either
regular sync functions or `async def` coroutines — both are supported.

Production-hardening in this module:
- Optional TLS (pass `ssl_context=` built with a loaded certificate chain)
- Configurable response codec (JSON / compact JSON / binary MessagePack)
- Keep-alive: a connection stays open and serves multiple requests in
  sequence, matching AsyncAgentClient's connection pool so repeat calls
  don't pay a new handshake each time.
"""

import asyncio
import inspect
import ssl as ssl_module
from typing import Any, Callable, Dict, Optional

from ..protocol import Message, MessageType, DEFAULT_CODEC
from .transport import send_frame_async, recv_frame_async
from ..state_sync import StateStore


class AsyncAgentServer:
    def __init__(
        self,
        host: str,
        port: int,
        agent_id: str,
        codec: int = DEFAULT_CODEC,
        ssl_context: Optional[ssl_module.SSLContext] = None,
    ):
        self.host = host
        self.port = port
        self.agent_id = agent_id
        self.codec = codec
        self.ssl_context = ssl_context
        self.methods: Dict[str, Callable[..., Any]] = {}
        self.events: Dict[str, Callable[..., Any]] = {}
        self._server: Optional[asyncio.base_events.Server] = None
        # Shared state (STATE_UPDATE messages), stored heap-ordered so
        # duplicate/replayed updates from any peer are idempotent.
        self.state = StateStore()

    def method(self, name: str):
        def decorator(fn: Callable[..., Any]):
            self.methods[name] = fn
            return fn
        return decorator

    def on_event(self, topic: str):
        """Register a handler for broadcast EVENT messages on a given topic.

        Handlers may be sync or `async def`. Return values are ignored by
        AsyncAgentBroadcaster (fire-and-forget); a minimal ack is still sent
        back so the connection can be reused/closed cleanly.
        """
        def decorator(fn: Callable[..., Any]):
            self.events[topic] = fn
            return fn
        return decorator

    async def start(self):
        self._server = await asyncio.start_server(
            self._handle, self.host, self.port, ssl=self.ssl_context
        )
        print(f"{self.agent_id} listening on {self.host}:{self.port}")
        async with self._server:
            await self._server.serve_forever()

    async def stop(self):
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        # Keep-alive loop: serve requests on this connection until the peer
        # closes it (or a fatal framing error occurs), so pooled clients can
        # reuse the same connection across many calls.
        try:
            while True:
                try:
                    raw = await recv_frame_async(reader)
                except asyncio.IncompleteReadError:
                    break  # peer closed the connection cleanly

                response = await self._process_one(raw)
                await send_frame_async(writer, response.to_bytes(codec=self.codec))
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _process_one(self, raw: bytes) -> Message:
        request: Optional[Message] = None
        try:
            request = Message.from_bytes(raw)

            if request.destination != self.agent_id:
                raise ValueError(
                    f"Message addressed to {request.destination}, "
                    f"not {self.agent_id}"
                )

            if request.type == MessageType.EVENT:
                topic = request.method
                payload = request.payload or {}
                if topic in self.events:
                    result = self.events[topic](**payload)
                    if inspect.isawaitable(result):
                        await result
                result = {"acked": True}
            elif request.type == MessageType.REQUEST:
                if request.method not in self.methods:
                    raise ValueError(f"Unknown method: {request.method}")
                fn = self.methods[request.method]
                payload = request.payload or {}
                result = fn(**payload)
                if inspect.isawaitable(result):
                    result = await result
            elif request.type == MessageType.HEARTBEAT:
                # Liveness check -- ack immediately, no user code involved.
                result = {"alive": True, "agent_id": self.agent_id}
            elif request.type == MessageType.STATE_UPDATE:
                # Shared-state sync -- store into the heap-backed StateStore.
                payload = request.payload or {}
                key = payload.get("key")
                if key is None:
                    raise ValueError("STATE_UPDATE payload requires a 'key'")
                item = self.state.apply(
                    request.source, key, payload.get("vector"),
                    priority=payload.get("priority"),
                )
                result = {"acked": True, "key": key, "seq": item.seq}
            else:
                raise ValueError("Expected request, event, heartbeat, or state_update message")

            return Message(
                version=1,
                type=MessageType.RESPONSE,
                source=self.agent_id,
                destination=request.source,
                request_id=request.request_id,
                parent_request_id=request.parent_request_id,
                method=request.method,
                payload=result,
            )

        except Exception as exc:
            return Message(
                version=1,
                type=MessageType.ERROR,
                source=self.agent_id,
                destination=request.source if request else "unknown",
                request_id=request.request_id if request else "unknown",
                parent_request_id=request.parent_request_id if request else None,
                method=request.method if request else None,
                error={
                    "type": type(exc).__name__,
                    "message": str(exc),
                },
            )

