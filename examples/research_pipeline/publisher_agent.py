"""Third agent in the research pipeline: takes a summary and 'publishes' it."""

import sys

from nova import AgentServer

# Host/port are OPTIONAL -- see researcher_agent.py for why. Pass explicit
# values only when you need a fixed/known port:
#   python publisher_agent.py 0.0.0.0 8203
args = sys.argv[1:]
if len(args) >= 2:
    args[1] = int(args[1])
server = AgentServer("publisher", *args)


@server.method("publish")
def publish(task_id, summary):
    print(f"[publisher] publishing report for {task_id}")
    return {
        "task_id": task_id,
        "status": "published",
        "report": summary,
    }


if __name__ == "__main__":
    server.start()
