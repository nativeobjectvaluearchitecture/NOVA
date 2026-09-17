NOVA, component by component
NOVA is a small agent-communication framework built around one idea: agents talk to each other over a lightweight, value-oriented protocol instead of heavy document-based APIs.

At a high level, NOVA has 3 layers:

Protocol layer: the wire format, message envelope, codecs, and framing
Capability/discovery layer: agent metadata and discovery
Runtime SDK layer: sync and async client/server APIs, broadcasting, orchestration, and shared-state sync
1) Public entry point
The public API is exposed from __init__.py.

This file re-exports everything from the lower-level modules, so users can do things like:

from nova import AgentClient
from nova import AsyncAgentServer
from nova import Message
from nova import SchemaRegistry
It acts as the single “import surface” for the library. In other words, it is the package façade.

2) Core wire protocol: protocol.py
This is the heart of NOVA.

Message
Message is the main network envelope for all agent traffic.

It contains:

version
type
source
destination
request_id
parent_request_id
method
payload
error
That means every message is a structured envelope with:

who sent it
who it is for
what request it belongs to
what method to invoke
what data to send
whether the result is an error
MessageType
These are fixed message kinds:

REQUEST
RESPONSE
ERROR
EVENT
HEARTBEAT
STATE_UPDATE
This gives NOVA a common language for:

calling a method on another agent
returning a result
returning an error
broadcasting a topic
checking liveness
syncing shared state
Codec
NOVA supports multiple wire encodings:

JSON_COMPACT
JSON_DYNAMIC
MSGPACK
MSGPACK_DYNAMIC
The important design choice is that the message is a positional list, not a dictionary. For example:

no repeated field names on the wire
smaller payloads
easier cross-language interchange
faster parsing
This is the “Native Object Value” idea: the message is not a bulky object schema repeatedly sent as JSON fields; it is compact positional data.

Why this matters
This makes NOVA well-suited to:

AI agent networks
fast message exchange
interoperability across languages
low-overhead transport
3) Framing: framing.py
NOVA uses a length-prefixed TCP framing scheme.

The file defines:

MAX_FRAME_SIZE = 50 * 1024 * 1024
This means:

each message is prefixed with a 4-byte big-endian length header
the receiver knows how many bytes to read
oversize messages are rejected before parsing
This is crucial because raw TCP streams are just byte streams; without length framing, parsing becomes ambiguous.

So:

protocol.py describes the message
framing.py describes how to segment that message on the network
4) Agent discovery and capability manifest: agent_card.py
This is the A2A-style discovery layer.

AgentCard
An AgentCard is a capability manifest such as:

id — agent identity
skills — methods the agent exposes
topics — events it listens for
version
metadata
The special method name is:

__agent_card__
This is a built-in discovery method. Instead of hardcoding agent addresses and method names, a client can ask:

“Who are you?”
“What can you do?”
Why it matters
This is very useful in dynamic multi-agent systems where:

agents may appear at runtime
the orchestrator does not know all capabilities up front
new agents can join a system without a central registry
NOVA supports that via AgentClient.discover(...).

5) Schema-based signed messages: schema_registry.py
This is the optional “strict schema” layer.

Schema
A Schema is a contract like:

schema id
version
fields in a fixed order
Example:

("sender", "receiver", "type", "payload")
That means a message’s values are positional, and the receiver maps them back to names using the negotiated schema.

SchemaRegistry
This tracks:

negotiated schemas
outgoing sequence numbers
seen message IDs
replay protection
timestamp staleness checks
HMAC signatures
It does:

handshake
signed message encoding
verification
reject replayed or stale messages
map positional values to field names
Why it exists
This layer adds security and consistency:

prevents tampering
prevents duplicates
prevents stale messages
ensures schema structure is shared and validated
This is separate from the normal Message protocol. It is an optional high-integrity protocol layer, not the base transport.

6) Shared heap and state sync: heap.py and state_sync.py
These are coordination utilities for ordering and merging state updates.

HeapItem
Represents a queued value with:

priority
seq
agent_id
payload
It uses a heap ordering so items are processed by priority.

SharedHeap
This is a thread-safe priority queue with:

deduplication by (agent_id, seq)
snapshot export
idempotent merge
This matters because in distributed agent systems, updates can arrive twice or out of order. The heap helps keep state deterministic.

StateStore
This is a higher-level state repository.

It stores:

latest values by key
full history via snapshots
last-writer-wins semantics
A state update is represented as:

key
vector
This lets agents maintain a consistent shared model without manual merge logic.

Why it matters
This is how NOVA supports:

event-driven coordination
state synchronization
heap-driven workflows
deduplicated updates
7) Sync SDK: sync
This is the blocking / thread-based client/server implementation.

server.py
AgentServer

Responsibilities:

bind to a TCP port
register RPC methods with @server.method("...")
register event handlers with @server.on_event("...")
accept incoming messages
dispatch them to methods
send a RESPONSE or ERROR
This is the standard “server side” of a NOVA agent.

client.py
AgentClient

Responsibilities:

address a remote agent by agent_id, host, and port
send a REQUEST
receive a RESPONSE
raise on ERROR
support retries and auth tokens
support discover() + capabilities()
This is the standard “client side” of a NOVA agent.

broadcast.py
AgentBroadcaster

Responsibilities:

maintain a list of subscribers
publish an event topic to many agents
send in parallel
return per-subscriber success/failure results
This is the pub/sub layer built on top of the one-to-one message format.

8) Async SDK: async_
This is the asyncio version of the same model.

server.py
AsyncAgentServer

This behaves like AgentServer, but using:

asyncio.start_server
concurrent async request handling
support for async def handlers
heartbeats
STATE_UPDATE processing
This is ideal for:

many concurrent connections
event-driven workloads
high-throughput agent apps
client.py
AsyncAgentClient

This is the async equivalent of AgentClient with:

connection pooling
async retry logic
heartbeat()
send_state_update()
This makes an async agent capable of calling many peers efficiently.

broadcast.py
AsyncAgentBroadcaster

Same idea as the sync broadcaster, but using asyncio.gather and async connections.

orchestrator.py
This is the coordination layer for multi-agent workflows.

It defines:

AgentRegistry
AsyncOrchestrator
AsyncWorkerAgent
This is important because it supports a pattern like:

orchestrator introduces several agents to each other
each agent contributes results
orchestrator merges them into a shared heap
sync snapshot is broadcast back to everyone
This is how NOVA models a coordinated system rather than just point-to-point calls.

9) Example in the repo: research_pipeline
The example pipeline is a clear demonstration of NOVA in action:

researcher_agent.py
summarizer_agent.py
publisher_agent.py
orchestrator.py
The flow is:

each agent starts an AgentServer
orchestrator uses AgentClient.discover(...)
it fetches each agent’s AgentCard
it checks which skills are available
it calls research, then summarize, then publish
This is a canonical “agent pipeline” built on NOVA.

10) How a request actually moves through NOVA
A normal request path looks like this:

Client creates Message
Message is serialized with a codec
Frame is length-prefixed
TCP socket sends bytes
Server reads frame
Server decodes Message
It checks destination and method
It invokes the registered function
It creates a RESPONSE or ERROR
It serializes and sends back
For discovery:

the client sends a request to *
server recognizes the reserved __agent_card__ method
returns the AgentCard
For state sync:

agent emits STATE_UPDATE
server stores it in StateStore
deduplication and ordering happen through the shared heap
11) The central design philosophy
NOVA is designed around a few core ideas:

small messages
positional value encoding
lightweight transport
agent discovery
event broadcasting
state reconciliation
async and sync support
multi-agent orchestration
It is not a general-purpose RPC framework like HTTP/JSON REST. It is a communication substrate for multi-agent systems.

12) In one sentence
NOVA is a tiny, high-performance agent-to-agent protocol and runtime: the message layer defines the wire contract, the client/server layer implements request/response and broadcasting, and the orchestration/state layer gives agent systems a shared and ordered coordination model.

If you want, I can also give you:

a “component map” of all files with one-line explanations
a “how to build your first NOVA agent” walkthrough
or a deeper explanation of the message format and schema layer specifically.
