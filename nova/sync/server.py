"""AgentServer: registers methods and serves them over the NOVA TCP transport.

Production-hardening in this module:
- Optional TLS (pass `ssl_context=` built with `ssl.SSLContext` +
  `load_cert_chain(...)`) to encrypt and authenticate connections.
- Configurable response codec (JSON / compact JSON / binary MessagePack).
"""

import socket
import ssl as ssl_module
import threading
from typing import Any, Callable, Dict, Optional

from ..agent_card import AGENT_CARD_METHOD, AgentCard
from ..protocol import Message, MessageType, DEFAULT_CODEC
from .transport import send_frame, recv_frame


class AgentServer:
    def __init__(
        self,
        agent_id: str,
        host: str = "0.0.0.0",
        port: int = 0,
        codec: int = DEFAULT_CODEC,
        ssl_context: Optional[ssl_module.SSLContext] = None,
        metadata: Optional[Dict[str, Any]] = None,
        auth_token: Optional[str] = None,
    ):
        """
        host: defaults to "0.0.0.0" (listen on every network interface, so
            the machine this runs on is reachable without knowing its own
            IP in advance).
        port: defaults to 0, which tells the OS to pick any free port
            automatically. After `start()`, `self.port` is updated to the
            real bound port.
        These are only defaults, not requirements; pass explicit values if
        you need a fixed/known port (e.g. for a Docker EXPOSE or a
        firewall rule).
        auth_token: optional shared secret. When set, every request, event,
            or discovery message must carry a matching "_auth_token" value
            in its payload or it is rejected before any method or the
            AgentCard is touched. Pair with `ssl_context` in production so
            the token isn't sent in plaintext.
        """
        self.host = host
        self.port = port
        self.agent_id = agent_id
        self.codec = codec
        self.ssl_context = ssl_context
        self.methods: Dict[str, Callable[..., Any]] = {}
        self.events: Dict[str, Callable[..., Any]] = {}
        self.metadata: Dict[str, Any] = metadata or {}
        self.auth_token = auth_token
        self._running = False

    def method(self, name: str):
        def decorator(fn: Callable[..., Any]):
            self.methods[name] = fn
            return fn
        return decorator

    def on_event(self, topic: str):
        """Register a handler for broadcast EVENT messages on a given topic.

        Unlike `method()`, event handlers are fire-and-forget from the
        publisher's point of view: the caller (AgentBroadcaster) does not
        block waiting for meaningful results, so return values are ignored.
        A minimal RESPONSE/ERROR is still sent back purely for framing --
        the connection needs a reply before it can be reused/closed.
        """
        def decorator(fn: Callable[..., Any]):
            self.events[topic] = fn
            return fn
        return decorator

    @property
    def agent_card(self) -> AgentCard:
        """This agent's A2A-style capability manifest: identity + skills +
        topics. Every AgentServer exposes this automatically over the wire
        via the `__agent_card__` method, so any client can discover what an
        agent can do without hardcoding method names ahead of time."""
        return AgentCard(
            id=self.agent_id,
            skills=sorted(self.methods.keys()),
            topics=sorted(self.events.keys()),
            metadata=self.metadata,
        )

    def start(self):
        self._running = True
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            # If port=0 was requested, the OS picked a free ephemeral port --
            # reflect the real bound port back onto self so callers (and the
            # printed banner below) see the actual address, not "0".
            self.host, self.port = server.getsockname()
            server.listen(128)
            print(f"{self.agent_id} listening on {self.host}:{self.port}")

            while self._running:
                conn, addr = server.accept()
                if self.ssl_context is not None:
                    conn = self.ssl_context.wrap_socket(conn, server_side=True)
                thread = threading.Thread(
                    target=self._handle,
                    args=(conn, addr),
                    daemon=True,
                )
                thread.start()

    def stop(self):
        self._running = False

    def _handle(self, conn: socket.socket, addr):
        with conn:
            request: Optional[Message] = None
            try:
                request = Message.from_bytes(recv_frame(conn))

                # "*" is a wildcard destination used only for the A2A-style
                # discovery handshake (AgentClient.discover /
                # AgentClient.capabilities): a caller who doesn't yet know
                # this agent's identity can still ask "who are you / what
                # can you do" before addressing it by name.
                is_discovery = (
                    request.destination == "*"
                    and request.method == AGENT_CARD_METHOD
                )
                if not is_discovery and request.destination != self.agent_id:
                    raise ValueError(
                        f"Message addressed to {request.destination}, "
                        f"not {self.agent_id}"
                    )

                # Shared-secret gate: applies to EVERY message type,
                # including the discovery handshake itself, so an
                # unauthenticated caller can neither enumerate this agent's
                # AgentCard nor invoke any method/event.
                payload = dict(request.payload or {})
                provided_token = payload.pop("_auth_token", None)
                if self.auth_token is not None and provided_token != self.auth_token:
                    raise PermissionError("unauthorized: missing or invalid auth token")

                if request.type == MessageType.EVENT:
                    topic = request.method
                    if topic in self.events:
                        self.events[topic](**payload)
                    result = {"acked": True}
                elif request.type == MessageType.REQUEST:
                    if request.method == AGENT_CARD_METHOD:
                        # Built-in A2A-style discovery handshake: any caller
                        # can ask "who are you / what can you do" before
                        # invoking a real skill.
                        result = self.agent_card.to_dict()
                    elif request.method not in self.methods:
                        raise ValueError(f"Unknown method: {request.method}")
                    else:
                        fn = self.methods[request.method]
                        result = fn(**payload)
                else:
                    raise ValueError("Expected request or event message")

                response = Message(
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
                response = Message(
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

            send_frame(conn, response.to_bytes(codec=self.codec))

