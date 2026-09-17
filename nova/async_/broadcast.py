"""Asyncio counterpart to AgentBroadcaster: publishes to many subscribers
concurrently using asyncio.gather instead of one thread per subscriber.
"""

import asyncio
import uuid
from typing import Any, Dict, List, Optional

from ..protocol import Message, MessageType, DEFAULT_CODEC
from .transport import send_frame_async, recv_frame_async
from ..sync.broadcast import Subscriber, PublishResult


class AsyncAgentBroadcaster:
    """Asynchronous, concurrent publisher.

    Usage:
        bus = AsyncAgentBroadcaster(source="orchestrator")
        bus.subscribe("task.completed", "logger", "127.0.0.1", 9001)
        bus.subscribe("task.completed", "notifier", "127.0.0.1", 9002)

        results = await bus.publish("task.completed", task_id="T-1")
    """

    def __init__(
        self,
        source: str,
        timeout: float = 10.0,
        codec: int = DEFAULT_CODEC,
    ):
        self.source = source
        self.timeout = timeout
        self.codec = codec
        self._subscribers: Dict[str, List[Subscriber]] = {}

    def subscribe(self, topic: str, agent_id: str, host: str, port: int) -> None:
        self._subscribers.setdefault(topic, []).append(
            Subscriber(agent_id, host, port)
        )

    def unsubscribe(self, topic: str, agent_id: str) -> None:
        subs = self._subscribers.get(topic, [])
        self._subscribers[topic] = [s for s in subs if s.agent_id != agent_id]

    def subscribers_for(self, topic: str) -> List[Subscriber]:
        return list(self._subscribers.get(topic, []))

    async def _send_one(self, sub: Subscriber, topic: str, payload: dict) -> PublishResult:
        request_id = f"EVT-{uuid.uuid4().hex[:12]}"
        message = Message(
            version=1,
            type=MessageType.EVENT,
            source=self.source,
            destination=sub.agent_id,
            request_id=request_id,
            parent_request_id=None,
            method=topic,
            payload=payload,
        )
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(sub.host, sub.port),
                timeout=self.timeout,
            )
            try:
                await asyncio.wait_for(
                    send_frame_async(writer, message.to_bytes(codec=self.codec)),
                    timeout=self.timeout,
                )
                await asyncio.wait_for(
                    recv_frame_async(reader), timeout=self.timeout
                )  # discard ack; fire-and-forget semantics
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
            return PublishResult(sub, ok=True)
        except Exception as exc:
            return PublishResult(sub, ok=False, error=str(exc))

    async def publish(self, topic: str, **payload: Any) -> List[PublishResult]:
        """Send `topic` with `payload` to every current subscriber concurrently.

        Never raises for individual subscriber failures -- inspect the
        returned PublishResult list for partial failures.
        """
        subs = self.subscribers_for(topic)
        if not subs:
            return []
        return await asyncio.gather(
            *(self._send_one(sub, topic, payload) for sub in subs)
        )
