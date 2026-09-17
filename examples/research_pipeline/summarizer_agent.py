"""Second agent in the research pipeline: takes findings and produces a summary."""

import sys

from nova import AgentServer

# Host/port are OPTIONAL -- see researcher_agent.py for why. Pass explicit
# values only when you need a fixed/known port:
#   python summarizer_agent.py 0.0.0.0 8202
args = sys.argv[1:]
if len(args) >= 2:
    args[1] = int(args[1])
server = AgentServer("summarizer", *args)


@server.method("summarize")
def summarize(task_id, findings):
    print(f"[summarizer] summarizing {len(findings)} findings for {task_id}")
    return {
        "task_id": task_id,
        "summary": " ".join(findings),
    }


if __name__ == "__main__":
    server.start()
