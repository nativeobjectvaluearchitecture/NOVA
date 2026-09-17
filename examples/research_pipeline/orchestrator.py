"""Orchestrator for the research pipeline: Researcher -> Summarizer -> Publisher.

This is the one example in this repo showing how to build a NOVA agent
pipeline: nothing in nova/ is specific to these agent names, ports, or
methods -- swap in your own freely.

Run researcher_agent.py, summarizer_agent.py, and publisher_agent.py first (each in
its own terminal, or see run_all.sh), then run this script.

NOVA speaks an A2A-style handshake (like Google's Agent2Agent protocol): the
orchestrator doesn't need to already know each agent's identity or skill
list -- it connects to host:port, calls `AgentClient.discover(...)`, and the
remote agent replies with its `AgentCard` (id + skills + topics). Only then
does the orchestrator invoke an actual skill, and it can check
`client.card.has_skill(...)` first instead of hoping the method exists.
"""

import sys

from nova import AgentClient

# No IP/port is hardcoded here. Each agent's address is passed in as a
# command-line argument -- researcher, summarizer, and publisher can each
# live on a completely different machine, e.g.:
#   python orchestrator.py 10.1.2.199 8201 10.4.7.9 8202 10.9.0.14 8203
researcher = AgentClient.discover(sys.argv[1], int(sys.argv[2]), source="orchestrator")
summarizer = AgentClient.discover(sys.argv[3], int(sys.argv[4]), source="orchestrator")
publisher = AgentClient.discover(sys.argv[5], int(sys.argv[6]), source="orchestrator")

for client in (researcher, summarizer, publisher):
    print(f"discovered agent card: {client.card}")

task_id = "TASK-RESEARCH-001"

assert researcher.card.has_skill("research")
research_result = researcher.call(
    "research",
    task_id=task_id,
    topic="multi-agent communication protocols",
)

assert summarizer.card.has_skill("summarize")
summary_result = summarizer.call(
    "summarize",
    task_id=task_id,
    findings=research_result["findings"],
)

assert publisher.card.has_skill("publish")
publish_result = publisher.call(
    "publish",
    task_id=task_id,
    summary=summary_result["summary"],
)

print(publish_result)
