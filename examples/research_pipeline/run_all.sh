#!/usr/bin/env bash
# Launches the three research-pipeline agents in the background, then runs the
# orchestrator. Kills the agents when the orchestrator finishes.
#
# NOVA is purely a communication protocol -- it has no idea, and doesn't care,
# whether the researcher, summarizer, and publisher agents are three processes
# on this laptop, or three separate machines/containers on different networks,
# e.g.:
#   researcher  -> 10.1.2.199:8201
#   summarizer  -> 10.4.7.9:8202
#   publisher   -> 10.9.0.14:8203
#
# This script is only a convenience for the *single-machine* demo. For a real
# distributed deployment, don't use this script at all: start each
# *_agent.py directly on its own host, passing host and port as command-line
# arguments, e.g. on the researcher's machine:
#   python researcher_agent.py 0.0.0.0 8201
# ...and then run orchestrator.py wherever you like, pointing at each agent's
# real address:
#   python orchestrator.py 10.1.2.199 8201 10.4.7.9 8202 10.9.0.14 8203
set -e
cd "$(dirname "$0")"

RESEARCHER_HOST=127.0.0.1
RESEARCHER_PORT=8201
SUMMARIZER_HOST=127.0.0.1
SUMMARIZER_PORT=8202
PUBLISHER_HOST=127.0.0.1
PUBLISHER_PORT=8203

python researcher_agent.py "$RESEARCHER_HOST" "$RESEARCHER_PORT" &
RESEARCHER_PID=$!
python summarizer_agent.py "$SUMMARIZER_HOST" "$SUMMARIZER_PORT" &
SUMMARIZER_PID=$!
python publisher_agent.py "$PUBLISHER_HOST" "$PUBLISHER_PORT" &
PUBLISHER_PID=$!

cleanup() {
  kill "$RESEARCHER_PID" "$SUMMARIZER_PID" "$PUBLISHER_PID" 2>/dev/null || true
}
trap cleanup EXIT

sleep 0.5
python orchestrator.py "$RESEARCHER_HOST" "$RESEARCHER_PORT" "$SUMMARIZER_HOST" "$SUMMARIZER_PORT" "$PUBLISHER_HOST" "$PUBLISHER_PORT"
