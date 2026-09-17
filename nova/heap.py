"""Shared priority-heap primitives used by orchestrator/worker fan-out.

HeapItem is the unit of data passed through the contribute -> heap -> sync
pipeline: any agent can push one, and every agent keeps its own local
mirror kept consistent via SharedHeap.

Ordering is (priority, seq): lower priority pops first, equal-priority
items stay FIFO within their own agent's stream, and payload is never
compared so mixed/uncomparable payloads never raise TypeError from heapq.

Deduplication key is (agent_id, seq), not a single global counter, since
each agent owns its own seq namespace. This means merges from independent
workers never collide, and re-delivering the same snapshot/update is safe.
"""

import heapq
import threading
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional


@dataclass
class HeapItem:
    priority: float
    seq: int
    agent_id: str
    payload: Any

    def __lt__(self, other: "HeapItem") -> bool:
        return (self.priority, self.seq) < (other.priority, other.seq)

    def key(self) -> tuple:
        return (self.agent_id, self.seq)

    def to_list(self) -> list:
        return [self.priority, self.seq, self.agent_id, self.payload]

    @classmethod
    def from_list(cls, data: list) -> "HeapItem":
        return cls(priority=data[0], seq=data[1], agent_id=data[2], payload=data[3])


class SharedHeap:
    """Thread-safe min-heap with idempotent merges.

    Works from both sync code (plain threading.Lock) and asyncio code -- the
    lock's critical sections are short and never held across an await.
    """

    def __init__(self) -> None:
        self._heap: List[HeapItem] = []
        self._seen: set = set()
        self._seq_counter = 0
        self._lock = threading.Lock()

    def push(self, agent_id: str, priority: float, payload: Any) -> HeapItem:
        """Create a new item (auto-assigned local seq) and push it."""
        with self._lock:
            self._seq_counter += 1
            item = HeapItem(priority, self._seq_counter, agent_id, payload)
            key = item.key()
            if key not in self._seen:
                self._seen.add(key)
                heapq.heappush(self._heap, item)
            return item

    def merge(self, items: Iterable[HeapItem]) -> int:
        """Merge externally-created items (e.g. from a snapshot). Returns
        the count of items actually added (duplicates are skipped)."""
        added = 0
        with self._lock:
            for item in items:
                key = item.key()
                if key not in self._seen:
                    self._seen.add(key)
                    heapq.heappush(self._heap, item)
                    added += 1
        return added

    def pop(self) -> Optional[HeapItem]:
        with self._lock:
            return heapq.heappop(self._heap) if self._heap else None

    def peek(self) -> Optional[HeapItem]:
        with self._lock:
            return self._heap[0] if self._heap else None

    def snapshot(self) -> List[list]:
        """Sorted, wire-ready snapshot of every item currently held."""
        with self._lock:
            return [i.to_list() for i in sorted(self._heap)]

    def __len__(self) -> int:
        with self._lock:
            return len(self._heap)
