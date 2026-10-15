#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${WAYPILOT_PYTHON:?Set WAYPILOT_PYTHON to an absolute Python 3 executable}"
cargo fmt --all -- --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
"$WAYPILOT_PYTHON" -m py_compile fixtures/tools.py scripts/check_otel.py
cargo build --locked
"$WAYPILOT_PYTHON" scripts/check_otel.py target/debug/waypilot
