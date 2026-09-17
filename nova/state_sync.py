"""State synchronization for NOVA agents, backed by SharedHeap.

Any agent can push a (key, vector) update, and every agent keeps a locally
consistent view of shared state. Updates are stored as HeapItems in a
SharedHeap rather than a flat dict:

  - each update carries a priority (defaults to wall-clock time), so the
    heap orders updates oldest-first
  - duplicate delivery is a no-op: SharedHeap dedups on (agent_id, seq), so
    retried/replayed STATE_UPDATE messages never double-apply
  - the full update history stays available via snapshot() for replay or
    audit; get(key) returns the last-writer-wins current value in O(1)

StateStore only handles storage, not networking. AsyncAgentServer applies
incoming STATE_UPDATE messages into its own StateStore, and
AsyncAgentClient.send_state_update(...) is the sender-side helper.
"""

import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from .heap import HeapItem, SharedHeap


@dataclass
class StateUpdate:
    key: str
    vector: Any

    def to_payload(self) -> dict:
        return {"key": self.key, "vector": self.vector}

    @classmethod
    def from_payload(cls, payload: dict) -> "StateUpdate":
        return cls(key=payload["key"], vector=payload.get("vector"))


class StateStore:
    """Shared state kept as heap-ordered updates, with last-writer-wins reads."""

    def __init__(self) -> None:
        self._heap = SharedHeap()
        self._latest: Dict[str, HeapItem] = {}

    def apply(
        self,
        agent_id: str,
        key: str,
        vector: Any,
        priority: Optional[float] = None,
    ) -> HeapItem:
        """Record a STATE_UPDATE from `agent_id` and push it onto the heap.

        Returns the HeapItem that was created, even if it turned out to be
        a duplicate for that agent (duplicates are deduped inside the heap;
        this just reports what was submitted).
        """
        priority = time.time() if priority is None else priority
        payload = StateUpdate(key=key, vector=vector).to_payload()
        item = self._heap.push(agent_id, priority, payload)
        self._remember_latest(item)
        return item

    def merge(self, items: Iterable[HeapItem]) -> int:
        """Merge externally-received items (e.g. a peer's snapshot)."""
        items = list(items)
        added = self._heap.merge(items)
        for item in items:
            self._remember_latest(item)
        return added

    def merge_wire(self, rows: Iterable[list]) -> int:
        """Merge a wire-format snapshot (list-of-lists from `snapshot()`)."""
        return self.merge(HeapItem.from_list(row) for row in rows)

    def _remember_latest(self, item: HeapItem) -> None:
        key = item.payload.get("key") if isinstance(item.payload, dict) else None
        if key is None:
            return
        current = self._latest.get(key)
        if current is None or (item.priority, item.seq) >= (current.priority, current.seq):
            self._latest[key] = item

    def get(self, key: str, default: Any = None) -> Any:
        """Last-writer-wins current value for `key`."""
        item = self._latest.get(key)
        if item is None:
            return default
        return item.payload.get("vector")

    def keys(self) -> List[str]:
        return list(self._latest.keys())

    def snapshot(self) -> List[list]:
        """Full, wire-ready update history (sorted by priority, seq)."""
        return self._heap.snapshot()

    def __len__(self) -> int:
        return len(self._heap)

    def __contains__(self, key: str) -> bool:
        return key in self._latest
