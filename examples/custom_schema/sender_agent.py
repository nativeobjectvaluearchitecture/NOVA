"""Sender side of a fully user-defined NOVA schema (see receiver_agent.py).

Usage:
    python sender_agent.py 127.0.0.1 9300
"""

import sys
import time
import uuid

from nova import Schema
from nova import SchemaAgent

SHARED_SECRET = b"demo-shared-secret"

MY_SCHEMA = Schema(
    schema_id="CUSTOM-PIPELINE",
    version=1,
    fields=("message_id", "source", "timestamp", "destination", "payload"),
)


def main(host: str, port: int) -> None:
    agent = SchemaAgent(MY_SCHEMA, secret=SHARED_SECRET)
    agent.connect(host, port)
    print(f"connected to {host}:{port}")

    for i in range(3):
        agent.send(
            message_id=str(uuid.uuid4()),
            source="analyst",
            timestamp=time.time(),
            destination="build",
            payload={"project": "SSPC", "step": i},
        )
        print(f"sent message {i}")
        time.sleep(0.5)

    agent.close()


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9300
    main(host, port)
