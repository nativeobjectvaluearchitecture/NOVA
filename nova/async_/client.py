"""AsyncAgentClient: non-blocking counterpart to AgentClient.

Same request/response contract as the sync client, but built on asyncio so a
single agent process can have many in-flight calls to other agents at once
without blocking threads.

Production-hardening in this module:
- Optional TLS (pass `ssl_context=`)
- Configurable codec (JSON / compact JSON / binary MessagePack)
- Automatic retries with exponential backoff
- A lightweight per-destination connection pool so repeated calls to the
  same agent reuse warm TCP (or TLS) connections instead of paying a new
  handshake every time.
"""

import asyncio
import ssl as ssl_module
import uuid
from typing import Any, Optional

from ..protocol import Message, MessageType, DEFAULT_CODEC
from .transport import send_frame_async, recv_frame_async

_RETRYABLE_ERRORS = (
    ConnectionError,
    ConnectionRefusedError,
    ConnectionResetError,
    asyncio.TimeoutError,
    OSError,
)


class _ConnectionPool:
    """Keeps a small set of idle (reader, writer) pairs per client instance.

    A connection is checked out for the duration of one call and returned to
    the pool afterwards. Broken connections are simply dropped rather than
    returned. This avoids a fresh TCP/TLS handshake on every single call
    while still being safe for concurrent use (each call gets its own
    connection; the pool just avoids throwing warm ones away).
    """

    def __init__(self, max_size: int = 32):
        self.max_size = max_size
        self._idle: list = []
        self._lock = asyncio.Lock()

    async def acquire(self, connect_fn):
        async with self._lock:
            if self._idle:
                return self._idle.pop()
        return await connect_fn()

    async def release(self, conn, healthy: bool):
        reader, writer = conn
        if not healthy or writer.is_closing():
            writer.close()
            return
        async with self._lock:
            if len(self._idle) < self.max_size:
                self._idle.append(conn)
                return
        writer.close()


class AsyncAgentClient:
    def __init__(
        self,
        agent: str,
        host: str,
        port: int,
        source: str,
        timeout: float = 30.0,
        codec: int = DEFAULT_CODEC,
        ssl_context: Optional[ssl_module.SSLContext] = None,
        retries: int = 0,
        retry_backoff: float = 0.2,
        pool_size: int = 32,
    ):
        self.agent = agent
        self.host = host
        self.port = port
        self.source = source
        self.timeout = timeout
        self.codec = codec
        self.ssl_context = ssl_context
        self.retries = retries
        self.retry_backoff = retry_backoff
        self._pool = _ConnectionPool(max_size=pool_size)

    async def _connect(self):
        return await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port, ssl=self.ssl_context),
            timeout=self.timeout,
        )

    async def call(
        self,
        method: str,
        *,
        task_id: Optional[str] = None,
        parent_request_id: Optional[str] = None,
        **payload: Any,
    ) -> Any:
        request_id = f"REQ-{uuid.uuid4().hex[:12]}"

        if task_id is not None:
            payload.setdefault("task_id", task_id)

        message = Message(
            version=1,
            type=MessageType.REQUEST,
            source=self.source,
            destination=self.agent,
            request_id=request_id,
            parent_request_id=parent_request_id or task_id,
            method=method,
            payload=payload,
        )
        return await self._send(message)

    async def send_state_update(
        self,
        key: str,
        vector: Any,
        priority: Optional[float] = None,
    ) -> Any:
        """Push a shared-state update (diagram's STATE-UPDATE) to the peer.

        The peer stores it in its heap-backed `StateStore` (see
        `state_sync.py`); retried/duplicate delivery is idempotent because
        `SharedHeap` dedups on `(agent_id, seq)` on the receiving side --
        but note each *call* here is a fresh send, so retries of this same
        `call()` do get a new seq on the receiver. For true wire-level
        idempotency under retry, resend the exact same snapshot row via
        `merge`-style APIs instead of calling this repeatedly.
        """
        request_id = f"REQ-{uuid.uuid4().hex[:12]}"
        message = Message(
            version=1,
            type=MessageType.STATE_UPDATE,
            source=self.source,
            destination=self.agent,
            request_id=request_id,
            parent_request_id=None,
            method="__state_update__",
            payload={"key": key, "vector": vector, "priority": priority},
        )
        return await self._send(message)

    async def heartbeat(self) -> Any:
        """Send a liveness HEARTBEAT and return the peer's ack payload."""
        request_id = f"REQ-{uuid.uuid4().hex[:12]}"
        message = Message(
            version=1,
            type=MessageType.HEARTBEAT,
            source=self.source,
            destination=self.agent,
            request_id=request_id,
            parent_request_id=None,
            method="__heartbeat__",
            payload={},
        )
        return await self._send(message)

    async def start_heartbeat_loop(
        self, interval: float = 10.0, on_failure=None
    ) -> "asyncio.Task":
        """Spawn a background task sending heartbeats every `interval` seconds.

        Implements the diagram's periodic HEARTBEAT/ACK liveness check.
        `on_failure(exc)` (sync or async) is invoked whenever a heartbeat
        fails; the loop keeps retrying on the same interval regardless.
        Returns the asyncio.Task -- cancel it to stop the loop.
        """

        async def _loop():
            while True:
                try:
                    await self.heartbeat()
                except Exception as exc:  # noqa: BLE001 - report, don't crash loop
                    if on_failure is not None:
                        result = on_failure(exc)
                        if asyncio.iscoroutine(result):
                            await result
                await asyncio.sleep(interval)

        return asyncio.ensure_future(_loop())

    async def _send(self, message: Message) -> Any:
        request_id = message.request_id
        wire = message.to_bytes(codec=self.codec)

        attempt = 0
        last_exc: Optional[Exception] = None
        response: Optional[Message] = None

        while attempt <= self.retries:
            reader = writer = None
            healthy = True
            try:
                reader, writer = await self._pool.acquire(self._connect)
                await asyncio.wait_for(
                    send_frame_async(writer, wire), timeout=self.timeout
                )
                raw = await asyncio.wait_for(
                    recv_frame_async(reader), timeout=self.timeout
                )
                response = Message.from_bytes(raw)
                break
            except _RETRYABLE_ERRORS as exc:
                healthy = False
                last_exc = exc
                attempt += 1
                if attempt > self.retries:
                    raise ConnectionError(
                        f"Failed to reach {self.agent} at {self.host}:{self.port} "
                        f"after {attempt} attempt(s): {exc}"
                    ) from exc
                await asyncio.sleep(self.retry_backoff * (2 ** (attempt - 1)))
            finally:
                if writer is not None:
                    await self._pool.release((reader, writer), healthy)

        assert response is not None

        if response.request_id != request_id:
            raise RuntimeError(
                f"Request correlation failure: expected {request_id}, "
                f"received {response.request_id}"
            )

        if response.type == MessageType.ERROR:
            raise RuntimeError(response.error)

        return response.payload

