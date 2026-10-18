#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
binary="${WAYPILOT_BINARY:-target/debug/waypilot}"
if [[ ! -x "$binary" ]]; then
  cargo build --locked
fi
python="${WAYPILOT_SDK_PYTHON:-target/sdk-venv/bin/python}"
if [[ ! -x "$python" ]]; then
  printf 'Install requirements-dev.txt in target/sdk-venv first; see docs/agents.md.\n' >&2
  exit 1
fi
exec "$python" demos/agent_demo.py "$binary" "$@"
