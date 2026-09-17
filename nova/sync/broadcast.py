"""Pub/sub broadcast layer built on top of NOVA's point-to-point transport.

NOVA's core client/server is one-to-one request/response. This module adds a
one-to-many "publish" primitive on top of it: a publisher sends an EVENT
message to every subscriber registered for a topic, without blocking on each
one individually and without requiring every subscriber to be reachable for
the publish to be considered a success.

This is intentionally simple (in-memory subscriber list, no persistence, no
guaranteed delivery, no offline replay) -- for that, put a real message
broker (Kafka/RabbitMQ/NATS) behind the same `on_event()` handlers. What this
module gives you for free is broadcasting to many NOVA agents with zero
extra infrastructure.
"""

import socket
import threading
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..protocol import Message, MessageType, DEFAULT_CODEC
from .transport import send_frame, recv_frame


@dataclass
class Subscriber:
    agent_id: str
    host: str
    port: int


@dataclass
class PublishResult:
    """Per-subscriber outcome of a broadcast, so callers can see partial failures."""
    subscriber: Subscriber
    ok: bool
    error: Optional[str] = None


class AgentBroadcaster:
    """Synchronous, thread-parallel publisher.

    Usage:
        bus = AgentBroadcaster(source="orchestrator")
        bus.subscribe("task.completed", "logger", "127.0.0.1", 9001)
        bus.subscribe("task.completed", "notifier", "127.0.0.1", 9002)

        results = bus.publish("task.completed", task_id="T-1", status="done")
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
        self._lock = threading.Lock()

    def subscribe(self, topic: str, agent_id: str, host: str, port: int) -> None:
        with self._lock:
            self._subscribers.setdefault(topic, []).append(
                Subscriber(agent_id, host, port)
            )

    def unsubscribe(self, topic: str, agent_id: str) -> None:
        with self._lock:
            subs = self._subscribers.get(topic, [])
            self._subscribers[topic] = [s for s in subs if s.agent_id != agent_id]

    def subscribers_for(self, topic: str) -> List[Subscriber]:
        with self._lock:
            return list(self._subscribers.get(topic, []))

    def publish(self, topic: str, **payload: Any) -> List[PublishResult]:
        """Send `topic` with `payload` to every current subscriber in parallel.

        Returns one PublishResult per subscriber so the caller can inspect
        partial failures instead of the whole broadcast raising on the first
        unreachable agent.
        """
        subs = self.subscribers_for(topic)
        results: List[Optional[PublishResult]] = [None] * len(subs)
        threads = []

        def _send_one(index: int, sub: Subscriber):
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
                with socket.create_connection(
                    (sub.host, sub.port), timeout=self.timeout
                ) as sock:
                    send_frame(sock, message.to_bytes(codec=self.codec))
                    recv_frame(sock)  # discard ack; fire-and-forget semantics
                results[index] = PublishResult(sub, ok=True)
            except Exception as exc:
                results[index] = PublishResult(sub, ok=False, error=str(exc))

        for i, sub in enumerate(subs):
            t = threading.Thread(target=_send_one, args=(i, sub), daemon=True)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        return results  # type: ignore[return-value]
