"""NOVA public API.

The protocol itself (message format, framing, and shared data structures)
is defined independently of any particular concurrency style:

    nova.protocol       -- message envelope and wire codecs
    nova.framing        -- length-prefix framing shared by both transports
    nova.heap           -- shared priority-heap primitives
    nova.state_sync     -- shared-state store built on the heap
    nova.agent_card     -- capability discovery
    nova.schema_registry -- optional signed/custom message schemas

Two SDK bindings implement that same protocol using different concurrency
models, and neither depends on the other:

    nova.sync   -- blocking-socket client/server (AgentClient, AgentServer, ...)
    nova.async_ -- asyncio client/server (AsyncAgentClient, AsyncAgentServer, ...)

Everything below is re-exported at the top level for backward compatibility,
so `from nova import AgentClient` / `from nova import AsyncAgentClient` keep
working exactly as before the reorganization.
"""

from .sync import AgentClient, AgentServer, AgentBroadcaster, Subscriber, PublishResult, SchemaAgent
from .async_ import (
    AsyncAgentClient,
    AsyncAgentServer,
    AsyncAgentBroadcaster,
    AgentRegistry,
    AsyncOrchestrator,
    AsyncWorkerAgent,
)
from .protocol import Message, MessageType, Codec, DEFAULT_CODEC
from .framing import MAX_FRAME_SIZE
from .agent_card import AgentCard, AGENT_CARD_METHOD
from .schema_registry import (
    Schema,
    SchemaRegistry,
    SchemaError,
    UnknownSchemaError,
    SignatureError,
    ReplayError,
    StaleMessageError,
    DEFAULT_SCHEMA,
)
from .heap import HeapItem, SharedHeap
from .state_sync import StateStore, StateUpdate

__all__ = [
    "AgentClient",
    "AgentServer",
    "AsyncAgentClient",
    "AsyncAgentServer",
    "AgentBroadcaster",
    "AsyncAgentBroadcaster",
    "Subscriber",
    "PublishResult",
    "Message",
    "MessageType",
    "Codec",
    "DEFAULT_CODEC",
    "MAX_FRAME_SIZE",
    "AgentCard",
    "AGENT_CARD_METHOD",
    "Schema",
    "SchemaRegistry",
    "SchemaError",
    "UnknownSchemaError",
    "SignatureError",
    "ReplayError",
    "StaleMessageError",
    "DEFAULT_SCHEMA",
    "SchemaAgent",
    "HeapItem",
    "SharedHeap",
    "StateStore",
    "StateUpdate",
    "AgentRegistry",
    "AsyncOrchestrator",
    "AsyncWorkerAgent",
]
__version__ = "0.1.0"
