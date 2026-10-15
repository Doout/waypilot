#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${WAYPILOT_PYTHON:?Set WAYPILOT_PYTHON to an absolute Python 3 executable}"
cargo fmt --all -- --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
"$WAYPILOT_PYTHON" -m py_compile fixtures/*.py demos/*.py scripts/*.py
cargo build --locked
"$WAYPILOT_PYTHON" scripts/check_gateway.py target/debug/waypilot
"${WAYPILOT_SDK_PYTHON:-$WAYPILOT_PYTHON}" scripts/check_sdk.py target/debug/waypilot
"$WAYPILOT_PYTHON" scripts/check_otel.py target/debug/waypilot
