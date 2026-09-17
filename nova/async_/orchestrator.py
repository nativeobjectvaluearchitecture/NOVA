"""Async orchestrator/worker helpers for introducing agents and syncing a shared heap.

    1. The orchestrator introduces agents to each other (one-to-many
       session.start, carrying a roster of every agent's id/host/port).
    2. Agents report their initial findings back to the orchestrator
       (many-to-one contribute), which merges everything into a shared
       heap and re-broadcasts the merged snapshot (one-to-many heap.sync).
    3. From that point on, any two agents that were introduced can talk
       directly, one-on-one, without going through the orchestrator: each
       worker builds a small peers dict of AsyncAgentClients (one per
       roster entry) as soon as it receives session.start.
    4. The orchestrator is still available for steady-state fan-out
       (submit -> heap.update), but it is no longer a mandatory relay
       for agent-to-agent traffic.

This composes the existing client/server/broadcast/heap primitives with no
wire-protocol changes, just new method/topic names:

    EVENT  session.start  orchestrator -> all agents   {session_id, roster: [{agent_id,host,port}]}
    RPC    contribute     agent        -> orchestrator {session_id} -> {items:[{priority,data}]}
    EVENT  heap.sync      orchestrator -> all agents   {session_id, snapshot}
    RPC    submit         agent        -> orchestrator {priority,data} -> {ok, seq}
    EVENT  heap.update    orchestrator -> all agents   {session_id, item}
    RPC    <anything>     agent        -> agent (direct, no orchestrator involved)
    EVENT  <anything>     agent        -> agent (direct, no orchestrator involved)
"""

import asyncio
from typing import Any, Dict, Iterable, List, Optional

from .broadcast import AsyncAgentBroadcaster
from .client import AsyncAgentClient
from .server import AsyncAgentServer
from ..heap import HeapItem, SharedHeap


class AgentRegistry:
    """Simple id -> (host, port) directory the orchestrator fans out to."""

    def __init__(self) -> None:
        self._agents: Dict[str, tuple] = {}

    def register(self, agent_id: str, host: str, port: int) -> None:
        self._agents[agent_id] = (host, port)

    def all_ids(self) -> List[str]:
        return list(self._agents.keys())

    def items(self):
        return self._agents.items()

    def as_roster(self) -> List[Dict[str, Any]]:
        """Wire-ready roster: enough for any agent to directly `discover()`
        any other agent, without asking the orchestrator to relay."""
        return [
            {"agent_id": aid, "host": host, "port": port}
            for aid, (host, port) in self._agents.items()
        ]

    def __len__(self) -> int:
        return len(self._agents)


class AsyncOrchestrator:
    """Introduces agents to each other and coordinates a shared heap.

    Usage:
        registry = AgentRegistry()
        registry.register("researcher", "127.0.0.1", 8201)
        registry.register("summarizer", "127.0.0.1", 8202)

        orch = AsyncOrchestrator("orchestrator", registry, session_id="S-1",
                                  host="0.0.0.0", port=8200)
        asyncio.create_task(orch.serve_forever())  # so agents can `submit` back
        await orch.run_session()
    """

    def __init__(
        self,
        source: str,
        registry: AgentRegistry,
        session_id: str,
        contribute_timeout: float = 10.0,
        host: str = "0.0.0.0",
        port: Optional[int] = None,
    ):
        self.source = source
        self.registry = registry
        self.session_id = session_id
        self.contribute_timeout = contribute_timeout
        self.heap = SharedHeap()
        self.bus = AsyncAgentBroadcaster(source=source)
        self._clients: Dict[str, AsyncAgentClient] = {}

        for agent_id, (host_, port_) in registry.items():
            self.bus.subscribe("session.start", agent_id, host_, port_)
            self.bus.subscribe("heap.sync", agent_id, host_, port_)
            self.bus.subscribe("heap.update", agent_id, host_, port_)
            self._clients[agent_id] = AsyncAgentClient(
                agent=agent_id, host=host_, port=port_, source=source
            )

        # Embedded server so worker agents can call back with `submit`
        # (many-to-one, steady-state) even though agent-to-agent traffic no
        # longer needs to go through the orchestrator at all.
        self._server: Optional[AsyncAgentServer] = None
        if port is not None:
            self._server = AsyncAgentServer(host=host, port=port, agent_id=source)
            self._server.method("submit")(self._on_submit_rpc)

    async def serve_forever(self):
        """Start the embedded server (only needed if workers call `submit` back)."""
        if self._server is None:
            raise RuntimeError("AsyncOrchestrator was constructed without host/port")
        await self._server.start()

    async def _on_submit_rpc(self, priority: float, data: Any):
        return await self.submit("submitter", priority, data)

    async def introduce(self):
        """Step 1: one-to-many -- introduce every agent to the full roster
        (id/host/port) so they can build direct peer connections themselves."""
        return await self.bus.publish(
            "session.start",
            session_id=self.session_id,
            roster=self.registry.as_roster(),
        )

    # Kept as an alias: "start_session" reads naturally from the caller's
    # side, "introduce" emphasizes what it does for the agents receiving it.
    start_session = introduce

    async def collect_contributions(self) -> Dict[str, Any]:
        """Step 2: many-to-one -- call `contribute` on every agent concurrently.

        A slow/unreachable agent contributes nothing rather than blocking the
        whole session (bounded by `contribute_timeout` per agent).
        """
        async def _one(agent_id: str, client: AsyncAgentClient):
            try:
                resp = await asyncio.wait_for(
                    client.call("contribute", session_id=self.session_id),
                    timeout=self.contribute_timeout,
                )
                for entry in resp.get("items", []):
                    self.heap.push(agent_id, entry["priority"], entry["data"])
                return agent_id, resp
            except Exception as exc:
                return agent_id, {"error": str(exc)}

        results = await asyncio.gather(
            *(_one(agent_id, client) for agent_id, client in self._clients.items())
        )
        return dict(results)

    async def broadcast_sync(self):
        """Step 3: one-to-many -- push the merged heap snapshot to everyone."""
        return await self.bus.publish(
            "heap.sync", session_id=self.session_id, snapshot=self.heap.snapshot()
        )

    async def run_session(self) -> Dict[str, Any]:
        """Runs the full introduce -> collect -> sync pipeline once."""
        await self.introduce()
        contributions = await self.collect_contributions()
        await self.broadcast_sync()
        return contributions

    async def submit(self, submitter_id: str, priority: float, data: Any):
        """Steady-state: accept a new item and fan out a small delta update
        instead of a full re-sync."""
        item = self.heap.push(submitter_id, priority, data)
        await self.bus.publish(
            "heap.update", session_id=self.session_id, item=item.to_list()
        )
        return {"ok": True, "seq": item.seq}


class AsyncWorkerAgent(AsyncAgentServer):
    """Base class for a worker participating in the introduce/heap-sync
    protocol.

    On `session.start`, this automatically builds `self.peers`: a dict of
    `AsyncAgentClient`s, one per *other* agent in the roster, so you can call
    a peer directly:

        await self.peers["summarizer"].call("summarize", text=...)

    and register direct-call handlers on yourself just like any other skill:

        self.method("ask_peer")(self._on_ask_peer)

    -- no orchestrator involvement required for that traffic at all.
    Override `contribute()` for the orchestrator's initial data-gathering
    round; everything else (session bookkeeping, heap merge on sync/update,
    peer-client setup) is handled for you.
    """

    def __init__(
        self,
        host: str,
        port: int,
        agent_id: str,
        orchestrator_host: str,
        orchestrator_port: int,
        orchestrator_id: str = "orchestrator",
    ):
        super().__init__(host=host, port=port, agent_id=agent_id)
        self.heap = SharedHeap()
        self.session_id: Optional[str] = None
        self.roster: List[Dict[str, Any]] = []
        self.peers: Dict[str, AsyncAgentClient] = {}
        self._orchestrator = AsyncAgentClient(
            agent=orchestrator_id,
            host=orchestrator_host,
            port=orchestrator_port,
            source=agent_id,
        )

        self.on_event("session.start")(self._on_session_start)
        self.on_event("heap.sync")(self._on_heap_sync)
        self.on_event("heap.update")(self._on_heap_update)
        self.method("contribute")(self._contribute_wrapper)

    async def _on_session_start(self, session_id: str, roster: List[Dict[str, Any]]):
        """The orchestrator's introduction: remember the session and build a
        direct client for every *other* agent so peer-to-peer calls need no
        further discovery step."""
        self.session_id = session_id
        self.roster = roster
        self.peers = {
            entry["agent_id"]: AsyncAgentClient(
                agent=entry["agent_id"],
                host=entry["host"],
                port=entry["port"],
                source=self.agent_id,
            )
            for entry in roster
            if entry["agent_id"] != self.agent_id
        }

    async def _on_heap_sync(self, session_id: str, snapshot: List[list]):
        self.heap.merge(HeapItem.from_list(x) for x in snapshot)

    async def _on_heap_update(self, session_id: str, item: list):
        self.heap.merge([HeapItem.from_list(item)])

    async def _contribute_wrapper(self, session_id: str):
        result = self.contribute(session_id)
        if asyncio.iscoroutine(result):
            result = await result
        return result

    def contribute(self, session_id: str) -> Dict[str, Any]:
        """Override in subclasses. Must return {"items": [{"priority", "data"}, ...]}."""
        return {"items": []}

    async def call_peer(self, agent_id: str, method: str, **payload: Any) -> Any:
        """Direct one-on-one call to another agent introduced via the
        orchestrator's roster -- bypasses the orchestrator entirely."""
        if agent_id not in self.peers:
            raise KeyError(
                f"{agent_id!r} is not a known peer; it wasn't in the roster "
                f"from session.start (or session.start hasn't fired yet)"
            )
        return await self.peers[agent_id].call(method, **payload)

    async def emit_new_item(self, priority: float, data: Any):
        """Push a new item back through the orchestrator so it gets fanned
        out to every agent as a `heap.update` (use this for data that
        genuinely needs to reach everyone; use `call_peer` for one-on-one)."""
        return await self._orchestrator.call("submit", priority=priority, data=data)
