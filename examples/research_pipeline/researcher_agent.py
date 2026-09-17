"""First agent in the research pipeline, showing the standard shape of a NOVA agent:
an AgentServer exposing one or more methods that other agents can call.

This agent exposes a 'research' method that a caller can invoke to get back a
(fake) list of findings for a topic.
"""

import sys

from nova import AgentServer

# Host/port are OPTIONAL. If you don't pass them, AgentServer defaults to
# host="0.0.0.0" (reachable on every network interface -- works the same
# whether you're on bare metal, a VM, or inside a Docker container) and
# port=0 (the OS picks any free port automatically). The real bound address
# is printed at startup and reported in this agent's AgentCard, so a caller
# using AgentClient.discover(...) never needs to know it in advance.
#
# Pass explicit values only when you need a fixed/known port, e.g. a Docker
# `EXPOSE 8201` or a firewall rule:
#   python researcher_agent.py 0.0.0.0 8201
args = sys.argv[1:]
if len(args) >= 2:
    args[1] = int(args[1])
server = AgentServer("researcher", *args)


@server.method("research")
def research(task_id, topic):
    print(f"[researcher] researching '{topic}' for {task_id}")
    return {
        "task_id": task_id,
        "topic": topic,
        "findings": [
            f"{topic} has grown steadily over the last year.",
            f"Three independent sources confirm interest in {topic}.",
        ],
    }


if __name__ == "__main__":
    server.start()
