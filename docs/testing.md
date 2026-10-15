# Testing and benchmarks

## Verification

Follow the [README](../README.md#test-and-benchmark) to install test dependencies, then run `bash scripts/verify.sh`. It runs formatting, Clippy, Rust tests, live JSON/SSE and stdio checks, the official Python MCP SDK, OTLP validation and a 64-client/32-call scale test.

CI runs on Linux and macOS. Linux also runs the 1,000-client/500-call release test. Artifacts include `scale-smoke.json`, `benchmark.json`, `gateway-benchmark.json` and `mcp-demo.json`, plus Linux `scale-qualification.json`. Reports upload on failure too.

## Load test

Run on Linux or macOS with other builds and benchmarks stopped. The runner starts local peers, creates temporary credentials and state, and stops its processes afterward. It raises the soft descriptor limit when possible and rejects an insufficient hard limit. It sends no traffic to public servers.

```sh
cargo build --locked --release
python3 load/qualify.py target/release/waypilot \
  --clients 1000 --concurrency 500 --seconds 60 --rate 500 \
  --output target/scale-qualification.json
```

Repeat with `--trace-ratio 0.1` and a different output path to start a local collector and collect sampled OTLP spans. Use `--seconds 300` for five minutes. The tracing-off run clears ambient OTLP settings.

The test keeps 1,000 TCP connections and MCP sessions open. Capacity phases require 500 calls active at the upstream together before release. Burst callers start in batches of 16 with a 2 ms pause. `--ramp-ms 0` sends an unpaced burst; that mode intermittently received TCP resets on the macOS test host and is outside the zero-error profile.

The workflow case admits 500 workflows with 1,000 simultaneous child calls, followed by dependent effects and joins. It uses 1,000 global upstream slots. [service.toml](../examples/service.toml) sets 500 slots, which queues extra child calls.

The suite checks:

- Capacity, excess-request rejection, incomplete bodies, 60 KB payloads under a 16 MiB byte budget, and admin access during ordinary overload.
- Rate buckets, output identity, 500 same-key callers sharing one invocation, and separate results across users.
- Abandoned initialization, 500 cancellations/deadlines, and up to 64 effects with dropped responses and no duplicate execution.
- Three cycles of 1,000 sessions, idle expiry, active work surviving its idle TTL, and revocation with 484 calls queued behind a 16-call server quota.
- Persisted revocation after restart, graceful shutdown with a nonreading client, and remote cleanup after a stdio input error.

The sustained workload offers 500 calls/second against a tool with a 10 ms wait. It checks every output's user and nonce. Latency starts at the scheduled send time, so generator delay counts. Generator p99 lag above 250 ms, cleanup errors/drops or sampled gateway RSS above 512 MiB fail the run. `--max-rss-mb` changes the RSS gate. Reports retain every latency trial, generator lag, resource samples and trace counts. Failure snapshots may include synthetic test outputs.

Concurrency barriers and sustained throughput measure separate cases. The generator, peer and collector share the host. RSS and descriptor peaks are sampled. These results cover one node and local HTTP peers; they do not establish WAN/TLS performance, multi-hour stability, large stdio fleets or multi-replica behavior.

## Forwarding overhead

```sh
export WAYPILOT_PYTHON="$(command -v python3)"
python3 scripts/benchmark_gateway.py target/release/waypilot \
  --samples 200 > target/gateway-benchmark.json
```

This compares direct and gateway echo calls with the same caller, arguments and local server. It measures JSON and SSE separately, initializes sessions, warms up three calls and alternates measurement order. Reuse is disabled, outputs must match, and gateway upstream counts must equal the sample count. The rate bucket allows 10,000 calls/second with a 10,000-token burst.

The test includes Python HTTP/JSON work, gateway controls and another HTTP exchange. The peer closes each HTTP/1.0 connection. Output contains every trial and nearest-rank p50/p95. A difference of medians describes this workload; subtracting p95 values does not give p95 overhead. Trace export follows the environment, so keep tracing off/on runs separate.

## Parallelism and prewarming

```sh
target/release/waypilot demo --samples 20 > target/benchmark.json
```

This fixture compares serial, parallel and learned prewarming execution with equivalent outputs. Two stdio servers each run two dependent tools. Startup waits 60 ms, each tool waits 15 ms, and a simulated reasoning gap waits 100 ms. There is no model call.

Cold comparisons use fresh engines with reuse off. Elapsed time includes prewarming; the report counts training work separately. Reuse checks expect four calls on the first input, zero on repeat and two after a source-only change. Those checks demonstrate invalidation, not cold-start latency.

## Recorded results

Source `29847e2`, release 0.2.0-alpha.1, recorded October 15, 2026 on an Apple M2 Max with 12 CPUs and 32 GiB RAM, Rust 1.99.0 and Python 3.14.7. All 18 load phases passed in both runs below.

| Tracing | Duration / successful calls | Achieved calls/s | Latency p50 / p95 / p99, ms | Generator lag p99, ms | Peak gateway RSS, MiB |
| --- | --- | ---: | --- | ---: | ---: |
| Off | 60 s / 30,000 | 499.92 | 10.86 / 16.94 / 71.56 | 35.80 | 284.55 |
| 10% | 300 s / 150,000 | 499.98 | 11.02 / 22.26 / 66.10 | 22.65 | 300.78 |

The sampled run exported 61,556 graph-node spans with dependency, scheduler and execution timing fields. The different run lengths and host noise prevent treating these rows as an exporter-overhead estimate.

Forwarding results use 200 trials per response type, with tracing off. The baseline is `50e1801`.

| Response | Direct p50 / p95, ms | Gateway p50 / p95, ms | Median overhead baseline / current, microseconds |
| --- | --- | --- | --- |
| JSON | 0.418 / 0.557 | 0.668 / 0.889 | 274 / 250 |
| SSE | 0.485 / 0.592 | 0.744 / 0.863 | 364 / 259 |

Raw local files are `target/scale-release.json`, `target/scale-release-long.json`, `target/overhead-before.json` and `target/overhead-after.json`. Their JSON includes binary SHA-256, environment and trials. `target/` is ignored by Git. The [Linux/macOS CI run](https://github.com/Doout/waypilot/actions/runs/38743792386) provides independent artifacts for the same source. Linux passed the full target with 30,000 calls at 499.92 calls/second.
