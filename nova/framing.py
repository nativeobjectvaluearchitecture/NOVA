"""Protocol-level framing constants.

This is part of the wire protocol, not an SDK implementation detail: every
NOVA message on the wire is prefixed with a 4-byte big-endian length header,
and a receiver must reject a declared length above this limit before
attempting to read it.

Both `nova.sync.transport` and `nova.async_.transport` implement this same
framing scheme independently (one using blocking sockets, the other using
asyncio streams) and both import the limit from here, so the value stays a
single source of truth shared by both SDK bindings.
"""

MAX_FRAME_SIZE = 50 * 1024 * 1024  # 50 MB safety limit
