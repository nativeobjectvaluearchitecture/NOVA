"""Length-prefixed TCP framing used by the reference NOVA transport."""

import struct
import socket

from ..framing import MAX_FRAME_SIZE

__all__ = ["MAX_FRAME_SIZE", "send_frame", "recv_frame", "recv_exact"]


def send_frame(sock: socket.socket, payload: bytes) -> None:
    header = struct.pack("!I", len(payload))
    sock.sendall(header + payload)


def recv_frame(sock: socket.socket) -> bytes:
    header = recv_exact(sock, 4)
    size = struct.unpack("!I", header)[0]
    if size > MAX_FRAME_SIZE:
        raise ValueError("Frame exceeds 50 MB safety limit")
    return recv_exact(sock, size)


def recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("Connection closed while receiving data")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)



