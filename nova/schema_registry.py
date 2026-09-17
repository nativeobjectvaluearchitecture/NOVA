"""Optional signed, replay-protected messaging format with a user-defined schema.

Two agents negotiate a schema once (an id, version, and ordered field list).
After that, messages are sent as compact positional arrays instead of keyed
objects, so field names aren't repeated on every message.

Since positional arrays have no built-in authentication, each message here
is signed (HMAC-SHA256), sequenced (per sender/schema), timestamped, and
checked for replay before being accepted.

This is separate from nova.protocol.Message/Codec; it doesn't replace the
JSON/MessagePack codec used elsewhere in NOVA.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# Wire tags, kept distinct from nova.protocol.Codec tags in case both are
# used on the same socket/port.
TAG_SCHEMA_HANDSHAKE = 0x10
TAG_SCHEMA_MESSAGE = 0x11

DEFAULT_MAX_CLOCK_SKEW_SECONDS = 300  # 5 minutes


class SchemaError(Exception):
    """Base class for schema-registry errors."""


class UnknownSchemaError(SchemaError):
    """Raised when a message references a schema_id/version that was never negotiated."""


class SignatureError(SchemaError):
    """Raised when a message's HMAC signature doesn't match."""


class ReplayError(SchemaError):
    """Raised when a message_id/sequence has already been seen."""


class StaleMessageError(SchemaError):
    """Raised when a message's timestamp is outside the allowed clock-skew window."""


@dataclass(frozen=True)
class Schema:
    """An agreed field order for compact positional messages.

    schema_id + version identify a layout, e.g. Schema("AM-001", 1,
    ("sender", "receiver", "type", "payload")) means position 0 is
    "sender", position 1 is "receiver", and so on.
    """

    schema_id: str
    version: int
    fields: Tuple[str, ...]

    def index_of(self, field_name: str) -> int:
        return self.fields.index(field_name)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_id": self.schema_id,
            "version": self.version,
            "fields": list(self.fields),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Schema":
        return cls(
            schema_id=data["schema_id"],
            version=int(data["version"]),
            fields=tuple(data["fields"]),
        )

    def key(self) -> Tuple[str, int]:
        return (self.schema_id, self.version)


class SchemaRegistry:
    """Stores negotiated schemas and tracks replay state per sender/schema.

    Share one instance between the send and receive path of an agent: it
    holds both the outgoing sequence counters and the incoming "seen"
    tracking.
    """

    def __init__(self, max_clock_skew_seconds: int = DEFAULT_MAX_CLOCK_SKEW_SECONDS):
        self._schemas: Dict[Tuple[str, int], Schema] = {}
        self._send_sequence: Dict[str, int] = {}  # keyed by schema_id, for outgoing messages
        self._seen: Dict[Tuple[str, str], int] = {}  # (sender, schema_id) -> highest sequence seen
        self._seen_ids: set = set()  # message_id values already accepted (belt & suspenders)
        self.max_clock_skew_seconds = max_clock_skew_seconds

    # Handshake

    def register(self, schema: Schema) -> None:
        """Record a schema as negotiated (call on both sides)."""
        self._schemas[schema.key()] = schema

    def get(self, schema_id: str, version: int) -> Schema:
        try:
            return self._schemas[(schema_id, version)]
        except KeyError:
            raise UnknownSchemaError(
                f"Schema {schema_id!r} v{version} was never negotiated. "
                f"Perform a handshake before sending/receiving compact messages."
            )

    def encode_handshake(self, schema: Schema, secret: bytes) -> bytes:
        """Build a signed handshake frame announcing a schema.

        The receiver must verify the signature before trusting the schema,
        otherwise a malicious field ordering could be injected.
        """
        body = json.dumps(schema.to_dict(), separators=(",", ":")).encode("utf-8")
        sig = _sign(bytes([TAG_SCHEMA_HANDSHAKE]) + body, secret)
        return bytes([TAG_SCHEMA_HANDSHAKE]) + body + sig

    def decode_handshake(self, data: bytes, secret: bytes) -> Schema:
        if not data or data[0] != TAG_SCHEMA_HANDSHAKE:
            raise SchemaError("Not a schema handshake frame")
        body, sig = data[1:-32], data[-32:]
        expected = _sign(bytes([TAG_SCHEMA_HANDSHAKE]) + body, secret)
        if not hmac.compare_digest(sig, expected):
            raise SignatureError("Schema handshake signature verification failed")
        schema = Schema.from_dict(json.loads(body.decode("utf-8")))
        self.register(schema)
        return schema

    # Compact signed messages

    def encode(
        self,
        schema: Schema,
        values: Dict[str, Any],
        secret: bytes,
        sender: str,
    ) -> bytes:
        """Encode `values` (a dict keyed by schema field names) as a signed,
        sequenced, positional array using the given schema.
        """
        if schema.key() not in self._schemas:
            raise UnknownSchemaError(
                f"Schema {schema.schema_id!r} v{schema.version} must be "
                f"registered (handshake) before encoding messages with it."
            )
        missing = [f for f in schema.fields if f not in values]
        if missing:
            raise SchemaError(f"Missing required schema fields: {missing}")

        seq_key = f"{sender}:{schema.schema_id}"
        sequence = self._send_sequence.get(seq_key, 0) + 1
        self._send_sequence[seq_key] = sequence

        envelope = {
            "schema_id": schema.schema_id,
            "version": schema.version,
            "message_id": uuid.uuid4().hex,
            "sender": sender,
            "sequence": sequence,
            "timestamp": time.time(),
            # Positional values, in the exact order the schema defines.
            "values": [values[f] for f in schema.fields],
        }
        body = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        sig = _sign(bytes([TAG_SCHEMA_MESSAGE]) + body, secret)
        return bytes([TAG_SCHEMA_MESSAGE]) + body + sig

    def decode(self, data: bytes, secret: bytes) -> Dict[str, Any]:
        """Verify signature, check replay/staleness, then map positional
        values back to field names using the referenced schema.

        Returns a plain dict, e.g. ``{"sender": "analyst", "receiver":
        "build", "type": "task", "payload": {...}}``.
        """
        if not data or data[0] != TAG_SCHEMA_MESSAGE:
            raise SchemaError("Not a schema-encoded message frame")

        body, sig = data[1:-32], data[-32:]
        expected = _sign(bytes([TAG_SCHEMA_MESSAGE]) + body, secret)
        if not hmac.compare_digest(sig, expected):
            raise SignatureError(
                "Message signature verification failed (tampering or wrong shared secret)"
            )

        envelope = json.loads(body.decode("utf-8"))
        schema = self.get(envelope["schema_id"], envelope["version"])

        # Replay / ordering check.
        sender = envelope["sender"]
        seq = envelope["sequence"]
        msg_id = envelope["message_id"]
        seen_key = (sender, schema.schema_id)
        last_seq = self._seen.get(seen_key, 0)
        if msg_id in self._seen_ids:
            raise ReplayError(f"Duplicate message_id {msg_id!r}")
        if seq <= last_seq:
            raise ReplayError(
                f"Sequence {seq} from {sender!r} is not greater than last "
                f"seen sequence {last_seq}"
            )
        self._seen[seen_key] = seq
        self._seen_ids.add(msg_id)

        # Staleness check.
        age = abs(time.time() - envelope["timestamp"])
        if age > self.max_clock_skew_seconds:
            raise StaleMessageError(
                f"Message timestamp is {age:.1f}s away from now, exceeding "
                f"allowed skew of {self.max_clock_skew_seconds}s"
            )

        values = envelope["values"]
        if len(values) != len(schema.fields):
            raise SchemaError(
                f"Schema {schema.schema_id!r} v{schema.version} expects "
                f"{len(schema.fields)} fields, message has {len(values)}"
            )

        result = dict(zip(schema.fields, values))
        result["_message_id"] = msg_id
        result["_sequence"] = seq
        result["_timestamp"] = envelope["timestamp"]
        return result


def _sign(data: bytes, secret: bytes) -> bytes:
    return hmac.new(secret, data, hashlib.sha256).digest()


# Default schema: [sender, receiver, type, payload]
DEFAULT_SCHEMA = Schema(
    schema_id="AM-001",
    version=1,
    fields=("sender", "receiver", "type", "payload"),
)
