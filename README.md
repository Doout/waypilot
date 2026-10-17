# Waypilot

Waypilot is an MCP gateway built on [Wayframe](https://github.com/Doout/wayframe). It routes calls to HTTP or stdio servers and runs configured workflows with parallel branches and joins. It has bearer authentication, tool grants, rate limits, result reuse, server prewarming and OpenTelemetry traces.

Cooperative upstreams can opt into [remote state validation](docs/gateway.md#remote-state-validation) and dependency-aware branch invalidation.

The [incident-agent workbench](docs/agents.md) runs an investigator and read-only reviewer
through real MCP tools, with a free policy mode, optional model mode and an uncached
benchmark baseline. Start it with `bash scripts/agent_demo.sh` after installing the test dependencies.

This is a single-node pre-release. Workflows come from TOML. Prewarming prepares servers without invoking tools.

## Try the demo

Requires Git, Rust 1.99.0 and Python 3.11+. The local demo needs no model key, Docker or paid service.

```sh
git clone git@github.com:Doout/waypilot.git
cd waypilot
export WAYPILOT_PYTHON="$(command -v python3)"
bash scripts/demo.sh
```

The demo starts local HTTP and stdio MCP servers, checks results and stops them. Its four-tool workflow makes four upstream calls, zero on an exact repeat, and two after changing one input. Add `--remote` to call [Microsoft Learn's public MCP server](https://learn.microsoft.com/en-us/training/support/mcp).

## Run the gateway

Run these commands in one shell:

```sh
export WAYPILOT_PYTHON="$(command -v python3)"
export WAYPILOT_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
export WAYPILOT_ADMIN_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
cargo build --locked

target/debug/waypilot serve --config examples/local.toml &
waypilot_pid=$!
until curl --silent --fail http://127.0.0.1:7400/healthz >/dev/null; do
  kill -0 "$waypilot_pid" || exit 1
  sleep 0.1
done

target/debug/waypilot list \
  --url http://127.0.0.1:7400/mcp --token-env WAYPILOT_TOKEN

target/debug/waypilot call \
  --url http://127.0.0.1:7400/mcp --token-env WAYPILOT_TOKEN \
  --tool waypilot__inspect \
  --args '{"manifest":"tokio serde","source":"fn main() {}"}'

kill "$waypilot_pid"
```

Choose a configuration:

| File | Use |
| --- | --- |
| [local.toml](examples/local.toml) | Local stdio servers and a workflow |
| [microsoft-learn.toml](examples/microsoft-learn.toml) | Public remote HTTP server |
| [service.toml](examples/service.toml) | Resource limits and persistent user controls. Replace the server URL before use. |

See the [gateway reference](docs/gateway.md) for clients, authentication, administration, reuse and tracing.
The shared [research note](https://github.com/Doout/wayframe/blob/main/docs/research.md) covers related work, design limits and proposed experiments.

## Test and benchmark

```sh
python3 -m venv target/sdk-venv
target/sdk-venv/bin/python -m pip install -r requirements-dev.txt
export WAYPILOT_PYTHON="$(command -v python3)"
export WAYPILOT_SDK_PYTHON="$PWD/target/sdk-venv/bin/python"
bash scripts/verify.sh

cargo build --locked --release
python3 scripts/benchmark_gateway.py target/release/waypilot \
  --samples 100 > target/gateway-benchmark.json
```

Verification includes Rust checks, live MCP peers, the official Python SDK, OTLP export and a 64-client scale test. Linux CI also tests 1,000 connected clients and 500 concurrent calls. [Testing and benchmarks](docs/testing.md) has the full load commands, measured results and test limits.

Apache-2.0.
