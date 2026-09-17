"""Synchronous (blocking-socket) NOVA implementation.

Everything here uses plain `socket` calls -- no event loop required. Good
fit for simple scripts, CLI tools, or code that doesn't already run under
asyncio. See `nova.async_` for the asyncio-based counterpart that shares
the exact same wire protocol (`nova.protocol.Message`).
"""

from .client import AgentClient
from .server import AgentServer
from .broadcast import AgentBroadcaster, Subscriber, PublishResult
from .schema_agent import SchemaAgent
from .transport import send_frame, recv_frame, recv_exact, MAX_FRAME_SIZE

__all__ = [
    "AgentClient",
    "AgentServer",
    "AgentBroadcaster",
    "Subscriber",
    "PublishResult",
    "SchemaAgent",
    "send_frame",
    "recv_frame",
    "recv_exact",
    "MAX_FRAME_SIZE",
]
