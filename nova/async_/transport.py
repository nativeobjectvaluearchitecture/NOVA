"""Asyncio length-prefixed framing, mirroring sync/transport.py for non-blocking I/O."""

import asyncio
import struct

from ..framing import MAX_FRAME_SIZE


async def send_frame_async(writer: asyncio.StreamWriter, payload: bytes) -> None:
    header = struct.pack("!I", len(payload))
    writer.write(header + payload)
    await writer.drain()


async def recv_frame_async(reader: asyncio.StreamReader) -> bytes:
    header = await reader.readexactly(4)
    size = struct.unpack("!I", header)[0]
    if size > MAX_FRAME_SIZE:
        raise ValueError("Frame exceeds 50 MB safety limit")
    return await reader.readexactly(size)
