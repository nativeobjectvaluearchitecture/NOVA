"""AgentClient: the object-oriented entry point agents use to call other agents.

Production-hardening in this module:
- Optional TLS (pass `ssl_context=ssl.create_default_context()` or a custom one)
- Configurable codec (JSON / compact JSON / binary MessagePack, see protocol.py)
- Automatic retries with exponential backoff for transient connection failures
"""

import socket
import ssl as ssl_module
import time
import uuid
from typing import Any, Optional

from ..agent_card import AGENT_CARD_METHOD, AgentCard
from ..protocol import Message, MessageType, DEFAULT_CODEC
from .transport import send_frame, recv_frame

# Errors worth retrying: the peer wasn't reachable/ready yet, or the
# connection dropped mid-flight. Application-level ERROR responses are
# NOT retried here (retrying a failed business call is the caller's choice).
_RETRYABLE_ERRORS = (
    ConnectionError,
    ConnectionRefusedError,
    ConnectionResetError,
    TimeoutError,
    socket.timeout,
    OSError,
)


class AgentClient:
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
        auth_token: Optional[str] = None,
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
        self.auth_token = auth_token

    def _connect(self) -> socket.socket:
        sock = socket.create_connection(
            (self.host, self.port),
            timeout=self.timeout,
        )
        if self.ssl_context is not None:
            sock = self.ssl_context.wrap_socket(sock, server_hostname=self.host)
        return sock

    @classmethod
    def discover(
        cls,
        host: str,
        port: int,
        source: str,
        **kwargs: Any,
    ) -> "AgentClient":
        """A2A-style discovery handshake.

        Connects to `host:port` without needing to already know the remote
        agent's identity or capabilities, fetches its `AgentCard` (id +
        skills + topics), and returns a ready-to-use `AgentClient`
        addressed to whatever identity the agent reports.

        The returned client has a `.card` attribute holding the discovered
        `AgentCard`, so callers can inspect `client.card.skills` before
        deciding what to call.
        """
        # Bootstrap client: destination "*" is a placeholder, since we don't
        # know the real agent id until the card comes back.
        probe = cls("*", host, port, source=source, **kwargs)
        card_dict = probe.call(AGENT_CARD_METHOD)
        card = AgentCard.from_dict(card_dict)

        client = cls(card.id, host, port, source=source, **kwargs)
        client.card = card
        return client

    def capabilities(self) -> AgentCard:
        """Fetch the remote agent's current `AgentCard` on demand (skills +
        topics it exposes), without needing a prior `discover()` call."""
        card_dict = self.call(AGENT_CARD_METHOD)
        return AgentCard.from_dict(card_dict)

    def call(
        self,
        method: str,
        *,
        task_id: Optional[str] = None,
        parent_request_id: Optional[str] = None,
        **payload: Any,
    ) -> Any:
        request_id = f"REQ-{uuid.uuid4().hex[:12]}"

        # task_id is used both for correlation (parent_request_id) and, since most
        # agent methods expect it as an explicit argument, forwarded in the payload.
        if task_id is not None:
            payload.setdefault("task_id", task_id)

        # Automatically attach the shared-secret auth token (if configured)
        # to every outgoing request/event, including the discovery probe in
        # discover(). The server strips this reserved key before it ever
        # reaches a user-defined method or the AgentCard.
        if self.auth_token is not None:
            payload["_auth_token"] = self.auth_token

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
        wire = message.to_bytes(codec=self.codec)

        attempt = 0
        last_exc: Optional[Exception] = None
        response: Optional[Message] = None
        while attempt <= self.retries:
            try:
                with self._connect() as sock:
                    send_frame(sock, wire)
                    response = Message.from_bytes(recv_frame(sock))
                break
            except _RETRYABLE_ERRORS as exc:
                last_exc = exc
                attempt += 1
                if attempt > self.retries:
                    raise ConnectionError(
                        f"Failed to reach {self.agent} at {self.host}:{self.port} "
                        f"after {attempt} attempt(s): {exc}"
                    ) from exc
                time.sleep(self.retry_backoff * (2 ** (attempt - 1)))

        assert response is not None  # loop always breaks with a response or raises

        if response.request_id != request_id:
            raise RuntimeError(
                f"Request correlation failure: expected {request_id}, "
                f"received {response.request_id}"
            )

        if response.type == MessageType.ERROR:
            raise RuntimeError(response.error)

        return response.payload

