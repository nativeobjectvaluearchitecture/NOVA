"""NOVA message envelope: the common representation for all agent-to-agent traffic.

Wire format
-----------
Every serialized message is a 1-byte codec tag followed by the encoded body.
This makes the format self-describing so a receiver in any language can
decode a message without prior negotiation. Every message is a positional
array; there is no keyed/object wire format, so field names are never
repeated on the wire:

    byte 0      meaning
    ------      -------------------------------------------------
    0x02        JSON, positional array (compact, still text)
    0x03        MessagePack, positional array (binary, cross-language)
    0x04        JSON, dynamic positional array (trailing nulls trimmed)
    0x05        MessagePack, dynamic positional array (trailing nulls trimmed)

The positional array order is fixed and must stay backward compatible:

    [v, type, src, dst, id, parent, method, payload, error]

The dynamic variants (0x04 / 0x05) use the same field order, but trailing
fields that are None are dropped before encoding. For example, a REQUEST
with no error and no parent encodes as 7 elements instead of 9. On decode,
the array is padded back out to 9 elements with None, so field meaning
never changes, only trailing nulls are omitted from the wire.

MessagePack has mature implementations in JavaScript/TypeScript, Go, Rust,
Java, C#, C/C++, Ruby, and more, so a message encoded with CODEC_MSGPACK
can be produced/consumed by an agent written in any of those languages.
"""

from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Optional, Union
import json

try:
    import msgpack
    _HAS_MSGPACK = True
except ImportError:  # pragma: no cover - msgpack is an optional extra
    msgpack = None
    _HAS_MSGPACK = False


class MessageType(IntEnum):
    REQUEST = 0
    RESPONSE = 1
    ERROR = 2
    EVENT = 3
    # Added for diagram-style connection liveness / shared-state sync.
    # Appended (not inserted) so existing wire values never shift.
    HEARTBEAT = 4
    STATE_UPDATE = 5


class Codec:
    JSON_COMPACT = 0x02
    MSGPACK = 0x03
    JSON_DYNAMIC = 0x04
    MSGPACK_DYNAMIC = 0x05


DEFAULT_CODEC = Codec.JSON_COMPACT


@dataclass
class Message:
    version: int
    type: MessageType
    source: str
    destination: str
    request_id: str
    parent_request_id: Optional[str]
    method: Optional[Union[str, int]]
    payload: Any = None
    error: Any = None

    def to_list(self) -> list:
        """Positional encoding: same fields, no repeated key names on the wire."""
        return [
            self.version,
            int(self.type),
            self.source,
            self.destination,
            self.request_id,
            self.parent_request_id,
            self.method,
            self.payload,
            self.error,
        ]

    def to_dynamic_list(self) -> list:
        """Positional encoding with trailing None fields trimmed off.

        Same field order as to_list(), but any run of None values at the
        end of the array is dropped. `_from_dynamic_list` pads the array
        back out to 9 elements on decode, so this only changes wire size,
        never field meaning.
        """
        values = self.to_list()
        while values and values[-1] is None:
            values.pop()
        return values

    def to_bytes(self, codec: int = DEFAULT_CODEC) -> bytes:
        """Serialize using the given codec, prefixed with a 1-byte codec tag.

        Codec.JSON_COMPACT (default): positional array as text JSON.
        Codec.MSGPACK: positional array as binary MessagePack, smallest and
            fastest to (de)serialize, decodable from any language with a
            MessagePack library. Requires the `msgpack` package.
        """
        if codec == Codec.MSGPACK:
            if not _HAS_MSGPACK:
                raise RuntimeError(
                    "msgpack codec requested but the 'msgpack' package is not "
                    "installed. Install it with: pip install msgpack"
                )
            body = msgpack.packb(self.to_list(), use_bin_type=True)
        elif codec == Codec.JSON_COMPACT:
            body = json.dumps(
                self.to_list(), separators=(",", ":"), ensure_ascii=False
            ).encode("utf-8")
        elif codec == Codec.JSON_DYNAMIC:
            body = json.dumps(
                self.to_dynamic_list(), separators=(",", ":"), ensure_ascii=False
            ).encode("utf-8")
        elif codec == Codec.MSGPACK_DYNAMIC:
            if not _HAS_MSGPACK:
                raise RuntimeError(
                    "msgpack codec requested but the 'msgpack' package is not "
                    "installed. Install it with: pip install msgpack"
                )
            body = msgpack.packb(self.to_dynamic_list(), use_bin_type=True)
        else:
            raise ValueError(f"Unknown codec: {codec!r}")

        return bytes([codec]) + body

    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        if not data:
            raise ValueError("Cannot decode an empty message")

        codec, body = data[0], data[1:]

        if codec == Codec.MSGPACK:
            if not _HAS_MSGPACK:
                raise RuntimeError(
                    "Received a msgpack-encoded message but the 'msgpack' "
                    "package is not installed. Install it with: "
                    "pip install msgpack"
                )
            obj = msgpack.unpackb(body, raw=False)
            return cls._from_list(obj)
        elif codec == Codec.JSON_COMPACT:
            obj = json.loads(body.decode("utf-8"))
            return cls._from_list(obj)
        elif codec == Codec.JSON_DYNAMIC:
            obj = json.loads(body.decode("utf-8"))
            return cls._from_dynamic_list(obj)
        elif codec == Codec.MSGPACK_DYNAMIC:
            if not _HAS_MSGPACK:
                raise RuntimeError(
                    "Received a msgpack-encoded message but the 'msgpack' "
                    "package is not installed. Install it with: "
                    "pip install msgpack"
                )
            obj = msgpack.unpackb(body, raw=False)
            return cls._from_dynamic_list(obj)
        else:
            raise ValueError(
                f"Unknown codec tag: {codec!r}. This message may have been "
                f"produced by an incompatible NOVA version."
            )

    @classmethod
    def _from_list(cls, obj: list) -> "Message":
        return cls(
            version=obj[0],
            type=MessageType(obj[1]),
            source=obj[2],
            destination=obj[3],
            request_id=obj[4],
            parent_request_id=obj[5],
            method=obj[6],
            payload=obj[7],
            error=obj[8],
        )

    @classmethod
    def _from_dynamic_list(cls, obj: list) -> "Message":
        """Inverse of `to_dynamic_list`: pad a trimmed array back to 9 fields."""
        padded = list(obj) + [None] * (9 - len(obj))
        return cls._from_list(padded)

