# NOVA

### Native Object Value Architecture

NOVA is a high-performance communication architecture designed for AI agent systems.

As AI systems evolve toward multiple specialized agents, efficient agent-to-agent communication becomes increasingly important. NOVA is designed to provide a lightweight, low-overhead communication layer for distributed and multi-agent AI systems.

## Why NOVA?

Traditional agent communication can introduce unnecessary overhead through repeated serialization, parsing, and heavyweight message representations.

NOVA focuses on:

- Low-overhead communication
- Efficient value-oriented data exchange
- Network-based agent communication
- High-frequency agent interactions
- Distributed AI agent systems
- Simple integration between specialized agents

## Core Concept

```
┌──────────────┐
│   AI Agent   │
└──────┬───────┘
       │
       ▼
┌──────────────────────┐
│        NOVA          │
│ Native Object Values │
└──────────┬───────────┘
           │
           ▼
        TCP Network
           │
           ▼
┌──────────────────────┐
│        NOVA          │
│ Native Object Values │
└──────────┬───────────┘
           │
           ▼
┌──────────────┐
│   AI Agent   │
└──────────────┘
```

NOVA separates agent logic from the communication layer, allowing agents to communicate without requiring the communication mechanism to be tightly coupled to the agent implementation.

## Example Architecture

```text
                 ┌──────────────┐
                 │  Build Agent │
                 └──────┬───────┘
                        │
                        │ NOVA
                        ▼
                 ┌──────────────┐
                 │  Test Agent  │
                 └──────┬───────┘
                        │
                        │ NOVA
                        ▼
                 ┌──────────────┐
                 │Security Agent│
                 └──────────────┘
```

Each agent can remain independently responsible for its own task while NOVA provides the communication foundation between them.

## Design Goals

NOVA is designed around a few core principles:

### 1. Low Overhead

Minimize unnecessary processing involved in agent-to-agent communication.

### 2. Native Value Exchange

Represent communication around native values rather than requiring every interaction to be expressed as large, heavyweight documents.

### 3. Network Native

Enable agents running as separate processes, services, containers, or machines to communicate over a network.

### 4. High Throughput

Support systems where agents may exchange information frequently and latency matters.

### 5. Simple Agent Integration

The communication layer should be easy to integrate into existing AI-agent architectures.

## Transport

NOVA currently uses **TCP** as its transport layer.

```
Application
     │
     ▼
   NOVA
     │
     ▼
    TCP
     │
     ▼
   Network
```

TCP provides reliable, ordered delivery while NOVA defines how agent communication is structured above the transport layer.

## NOVA vs Traditional Agent Communication

NOVA is designed with a different priority from conventional API-style agent communication.

| Area | Traditional Approach | NOVA |
|------|----------------------|------|
| Communication | API/message oriented | Agent communication oriented |
| Data model | Often document based | Native value oriented |
| Transport | HTTP/WebSocket/etc. | TCP |
| Overhead | Can be higher depending on stack | Designed for low overhead |
| Primary goal | General application APIs | High-performance agent systems |

> Performance depends on implementation, workload, network conditions, and message size. Benchmarks should be used for quantitative comparisons.

## Use Cases

NOVA can be used as a communication foundation for:

- Multi-agent AI systems
- Distributed AI agents
- Agent orchestration
- Coding-agent pipelines
- Build and test agent systems
- Security-agent workflows
- Internal agent meshes
- High-frequency agent communication



## Quick Start

> Implementation and installation instructions will be added as the NOVA SDK and protocol stabilize.

Conceptually, an agent can expose a NOVA endpoint and communicate with another agent:

```
Agent A
   │
   │ connect
   ▼
NOVA Endpoint
   │
   │ exchange values
   ▼
NOVA Endpoint
   │
   ▼
Agent B
```

## How to Use NOVA

Install the package locally (editable install, from the repo root):

```bash
pip install -e .
```

NOVA ships two implementations of the same client/server model, so you can
pick whichever fits your codebase — both speak the same wire protocol and
can talk to each other interchangeably.

### 1. Synchronous agents (plain sockets, no event loop)

**Server** — exposes one or more callable methods:

```python
from nova import AgentServer

server = AgentServer("127.0.0.1", 8101, agent_id="build")

@server.method("build")
def build(task_id: str, specification: dict) -> dict:
    # ... do the work ...
    return {"status": "ok", "task_id": task_id}

server.serve_forever()
```

**Client** — calls that method from another agent/process:

```python
from nova import AgentClient

build = AgentClient("build", "127.0.0.1", 8101, source="analyst")

result = build.call(
    "build",
    task_id="TASK-001",
    specification={"language": "python", "requirement": "add login"},
)
print(result)
```

### 2. Async agents (asyncio, many concurrent calls)

```python
import asyncio
from nova import AsyncAgentServer, AsyncAgentClient

async def main():
    server = AsyncAgentServer("127.0.0.1", 8101, agent_id="build")

    @server.method("build")
    async def build(task_id: str, specification: dict) -> dict:
        return {"status": "ok", "task_id": task_id}

    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.1)  # let the server bind

    client = AsyncAgentClient("build", "127.0.0.1", 8101, source="analyst")
    result = await client.call("build", task_id="TASK-001", specification={})
    print(result)

    await server.stop()
    server_task.cancel()

asyncio.run(main())
```

### 3. Broadcasting to many agents at once

```python
from nova import AgentBroadcaster

broadcaster = AgentBroadcaster(source="orchestrator")
broadcaster.add_subscriber("build", "127.0.0.1", 8101)
broadcaster.add_subscriber("test", "127.0.0.1", 8102)

results = broadcaster.publish("deploy_ready", version="1.4.0")
```

### 4. Agent discovery

Instead of hardcoding an agent's identity, connect and discover it first:

```python
from nova import AgentClient

client = AgentClient.discover("127.0.0.1", 8101, source="analyst")
print(client.agent)  # the remote agent's reported id
```

### 5. Orchestrating a fleet of worker agents (async)

```python
from nova import AsyncOrchestrator, AgentRegistry

registry = AgentRegistry()
registry.register("build", "127.0.0.1", 8101)
registry.register("test", "127.0.0.1", 8102)

orchestrator = AsyncOrchestrator(registry)
# orchestrator.dispatch(...) fans work out to registered workers
```

### Try the included example

The fastest way to see NOVA end-to-end is the three-agent research pipeline
in `examples/research_pipeline/`:

```bash
cd examples/research_pipeline
bash run_all.sh
```

See [examples/](examples/) for the full, runnable source of every pattern above.

## Documentation

The protocol documentation will cover:

- Connection lifecycle
- Agent identification
- Message structure
- Native value representation
- Request/response communication
- Error handling
- Connection management
- Protocol versioning
- Security considerations
- Performance benchmarks

## Roadmap

- [ ] Define the initial NOVA protocol specification
- [ ] Implement core communication layer
- [ ] Define native value representation
- [ ] Add agent discovery/connection mechanisms
- [ ] Add comprehensive tests
- [ ] Build multi-agent examples
- [ ] Publish benchmark suite
- [ ] Add SDKs for additional languages
- [ ] Stabilize protocol version 1.0

## Contributing

NOVA is intended to be an open-source project.

Contributions, ideas, benchmarks, protocol discussions, and implementation improvements are welcome.

Please read `CONTRIBUTING.md` before submitting a pull request.

## License

Apache License 2.0. See [LICENSE](LICENSE) for the full text.

Copyright 2026 Adeebullah Sheikh.

## Author

**Adeebullah Sheikh**

NOVA — Native Object Value Architecture

---

> Building the communication foundation for high-performance AI agent systems.
