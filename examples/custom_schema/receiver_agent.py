"""Receiver side of a fully user-defined NOVA schema.

Notice this schema's field order is NOT sender/receiver/type/payload --
it's whatever the user wants:

    [0] message_id   -- user's own business id (NOT the NOVA internal one)
    [1] source
    [2] timestamp
    [3] destination
    [4] payload

NOVA does not care. It only serializes/signs/frames/delivers positions in
this exact order, and hands back a dict keyed by these exact names.

Usage:
    python receiver_agent.py 127.0.0.1 9300
"""

import sys

from nova import Schema
from nova import SchemaAgent

# A shared secret both sides must know (in real deployments: env var / vault).
SHARED_SECRET = b"demo-shared-secret"

# The user's own schema -- notice the order/names are custom.
MY_SCHEMA = Schema(
    schema_id="CUSTOM-PIPELINE",
    version=1,
    fields=("message_id", "source", "timestamp", "destination", "payload"),
)


def main(host: str, port: int) -> None:
    agent = SchemaAgent(MY_SCHEMA, secret=SHARED_SECRET)
    agent.listen(host, port)
    print(f"receiver listening on {host}:{port}")
    agent.accept()
    print("client connected, waiting for messages...")

    for msg in agent.receive_forever():
        print(
            f"got message_id={msg['message_id']} "
            f"source={msg['source']} destination={msg['destination']} "
            f"payload={msg['payload']} (seq={msg['_sequence']})"
        )


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9300
    main(host, port)
