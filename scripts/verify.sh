#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${WAYPILOT_PYTHON:?Set WAYPILOT_PYTHON to an absolute Python 3 executable}"
cargo fmt --all -- --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
"$WAYPILOT_PYTHON" -m py_compile fixtures/*.py demos/*.py demos/agents/*.py scripts/*.py load/*.py
"${WAYPILOT_SDK_PYTHON:-$WAYPILOT_PYTHON}" -m ruff check load demos/agents demos/agent_demo.py fixtures/agent_barrier.py fixtures/agent_validation.py scripts/check_agents.py scripts/check_validation.py scripts/benchmark_agents.py scripts/benchmark_validation.py
"${WAYPILOT_SDK_PYTHON:-$WAYPILOT_PYTHON}" -m ruff format --check --quiet load demos/agents demos/agent_demo.py fixtures/agent_barrier.py fixtures/agent_validation.py scripts/check_agents.py scripts/check_validation.py scripts/benchmark_agents.py scripts/benchmark_validation.py
cargo build --locked
"$WAYPILOT_PYTHON" scripts/check_gateway.py target/debug/waypilot
"$WAYPILOT_PYTHON" scripts/check_state.py target/debug/waypilot
"${WAYPILOT_SDK_PYTHON:-$WAYPILOT_PYTHON}" scripts/check_sdk.py target/debug/waypilot
"${WAYPILOT_SDK_PYTHON:-$WAYPILOT_PYTHON}" scripts/check_agents.py target/debug/waypilot
"${WAYPILOT_SDK_PYTHON:-$WAYPILOT_PYTHON}" scripts/check_validation.py target/debug/waypilot
"${WAYPILOT_SDK_PYTHON:-$WAYPILOT_PYTHON}" load/agent_soak.py target/debug/waypilot --seconds 8 --wave-seconds 2 --rate 2 --concurrency 4 --trace-ratio 1 --output-dir target/agent-soak-smoke
"${WAYPILOT_SDK_PYTHON:-$WAYPILOT_PYTHON}" load/agent_faults.py target/debug/waypilot --iterations 1 --output target/agent-faults.json
"$WAYPILOT_PYTHON" scripts/check_otel.py target/debug/waypilot

"$WAYPILOT_PYTHON" load/qualify.py target/debug/waypilot --clients 64 --concurrency 32 --seconds 3 --rate 100 --trace-ratio 0.1 --output target/scale-smoke.json

"$WAYPILOT_PYTHON" load/state_qualify.py target/debug/waypilot --clients 64 --concurrency 32 --output target/state-scale-smoke.json

"${WAYPILOT_SDK_PYTHON:-$WAYPILOT_PYTHON}" load/observation_qualify.py target/debug/waypilot --clients 64 --concurrency 32 --output target/observation-scale-smoke.json
