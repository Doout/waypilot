#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
binary="${WAYPILOT_BINARY:-target/debug/waypilot}"
if command -v cargo >/dev/null 2>&1; then
  cargo build --locked
elif [[ ! -x "$binary" ]]; then
  printf 'Install Rust 1.99.0 and run cargo build --locked first.\n' >&2
  exit 1
fi
python="${WAYPILOT_PYTHON:-$(command -v python3)}"
exec "$python" demos/mcp_demo.py "$binary" --python "$python" "$@"
