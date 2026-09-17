"""AgentCard: a small capability manifest an agent hands out on request.

Similar to Google's A2A "Agent Card": every AgentServer automatically
exposes a built-in method, __agent_card__, that returns an AgentCard with:

    id        the agent's identity (the `destination` to address it as)
    skills    RPC methods it exposes (server.method(...))
    topics    broadcast/event topics it listens on (server.on_event(...))
    version   protocol version this agent speaks
    metadata  free-form extra info

A caller can connect to host:port without knowing the agent's identity or
capabilities in advance, call AgentClient.discover(host, port), and get
back both the card and a ready-to-use client.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# Well-known method name every NOVA agent responds to with its AgentCard.
# Kept short and namespaced (dunder-wrapped) so it can never collide with a
# real user-defined skill name.
AGENT_CARD_METHOD = "__agent_card__"


@dataclass
class AgentCard:
    """Self-describing capability manifest for a NOVA agent."""

    id: str
    skills: List[str] = field(default_factory=list)
    topics: List[str] = field(default_factory=list)
    version: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "skills": list(self.skills),
            "topics": list(self.topics),
            "version": self.version,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AgentCard":
        return cls(
            id=data["id"],
            skills=list(data.get("skills", [])),
            topics=list(data.get("topics", [])),
            version=data.get("version", 1),
            metadata=dict(data.get("metadata", {})),
        )

    def has_skill(self, name: str) -> bool:
        return name in self.skills

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return (
            f"AgentCard(id={self.id!r}, skills={self.skills!r}, "
            f"topics={self.topics!r})"
        )
