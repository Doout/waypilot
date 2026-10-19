# Testing and benchmarks

## Verification

Follow the [README](../README.md#test-and-benchmark) to install test dependencies, then run `bash scripts/verify.sh`. It runs formatting, Clippy, Rust tests, live JSON/SSE and stdio checks, the official Python MCP SDK, OTLP validation and a 64-client/32-call scale test.

Verification uses full trace sampling to require graph coverage deterministically.
Sampled load tests require request/invocation traces and validate graph timings when
graph spans are sampled; a small graph phase can legitimately have no sampled roots.

CI runs on Linux and macOS. Linux also runs the 1,000-client/500-call release test. Artifacts include `scale-smoke.json`, `benchmark.json`, `gateway-benchmark.json` and `mcp-demo.json`, plus Linux `scale-qualification.json`. Reports upload on failure too.

Linux repeats automatic pattern learning in three fresh trials of 1,000 agents at
concurrency 500. `learning-scale.json` records each trial; call or drain failures retain
gateway counters, learned rules and peer logs in `learning-scale.failure.json`.

The [incident-agent suite](agents.md#verify-and-compare) adds real multi-turn MCP callers,
policy decisions, read-only review, state mutation and required writes. Verification
runs its regression tests; CI also uploads `agent-demo.json` and `agent-benchmark.json`.

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

The observation-receipt scale test uses cooperative workbench tools. It captures graph
receipts on every session, uses 64 upstream slots behind 500 pending calls, applies one
guarded note per client, advances both metric revisions without changing values, rejects
every stale note, then recaptures and writes
once per client. It reconciles the upstream ledger, samples RSS and checks that receipt
storage and sessions drain. Concurrency here counts pending gateway calls.

```sh
target/sdk-venv/bin/python load/observation_qualify.py target/release/waypilot \
  --clients 1000 --concurrency 500 --output target/observation-scale.json
```

## Continuous HR soak

For a continuous one-hour comparison with changing agent behavior:

```sh
target/sdk-venv/bin/python load/hr_soak.py target/release/waypilot \
  --seconds 3600 --rate 2 --concurrency 128 --trace-ratio 0.1 \
  --require-uninterrupted \
  --output-dir target/hr-soak-$(date +%Y%m%d-%H%M%S)
```

Three isolated paths run together: direct MCP, guarded reuse without prediction, and
automatic learning from no rules. They receive identical seeded arrivals, pauses,
aliases, branches and external balance updates, with 64 upstream slots each. Reads vary
from a few milliseconds to three seconds; agent pauses range from zero to seven seconds.
Leave/benefits proportions shift every ten minutes. Some callers abandon after a profile,
request reads concurrently, reverse their order, or reread after an external update.
This is a synthetic policy workload over real local MCP connections, without a paid LLM.

The runner verifies each observation, compares per-conversation outputs across modes,
and reconciles every case write. It records raw conversations, one-minute learning and
resource snapshots, arrival-cohort latency, upstream work and bounded OTLP stage histograms.
It fails on unexpected errors, effect mismatches, resource growth or incomplete cleanup.
Output directories must be new. `--pause-scale 0.01 --seconds 18 --rate 1` provides a smoke
check; use the default pause scale for measurements. This tests realistic pacing rather
than the separate 1,000-connected-client/500-active-call capacity qualification.

`--require-uninterrupted` rejects a wall/monotonic-clock discrepancy above two seconds or
a monitor gap above ten seconds. Keep a laptop powered with its lid open; an idle-sleep
inhibitor cannot prevent lid-closed sleep. Clock samples and successful-run diagnostics
are retained. `--baseline /path/to/frozen/waypilot` adds the previous automatic predictor
as a fourth path with the same plan and budgets, alongside direct, guarded and current
automatic execution. Compare that path to isolate the runtime change from host/run noise.

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

## Remote state validation

```sh
python3 scripts/check_state.py target/debug/waypilot
python3 load/state_qualify.py target/release/waypilot \
  --clients 1000 --concurrency 500 --upstream-calls 64 \
  --output target/state-scale.json
```

The state suite covers JSON, SSE and stdio reads, unchanged-value version changes,
descendant invalidation, malformed evidence, cancellation, revocation, deadlines and
validation rate limits. The scale test keeps 1,000 sessions open and checks every workflow
result in four phases: cold, warm, a version-only change, and a value change. It issues
500-call bursts, allowing the documented token budget to refill between waves. Its elapsed
throughput includes pacing; request latency excludes that refill wait. This is a shared
identity/cache workload with changes between phases, not a sustained-throughput or atomic
cross-server snapshot claim. The existing qualification suite separately exercises offered load.

To compare with the exact pre-change code:

```sh
git worktree add --detach target/state-baseline d6c7e2c
CARGO_TARGET_DIR="$PWD/target/state-baseline-build" cargo build --locked --release \
  --manifest-path target/state-baseline/Cargo.toml
cargo build --locked --release
python3 scripts/benchmark_state.py target/release/waypilot \
  --baseline target/state-baseline-build/release/waypilot --samples 100 --read-ms 20 \
  > target/state-benchmark.json
```

The benchmark alternates four modes against owned HTTP peers: baseline TTL reuse, baseline
uncached execution, candidate TTL reuse, and candidate validated reuse. Each workflow has two
reads and two descendants. One resource changes outside the gateway every five requests.
An independent fingerprint oracle checks outputs; stale TTL hits are recorded, while any
incorrect validated or uncached result fails the run. Initialization is excluded. Reports
include binary hashes, raw timings, p50/p95/p99, business and validation calls, gateway CPU,
and RSS sampled after requests. CI uses the same candidate binary for the control modes;
the separate-binary comparison above measures code changes against `d6c7e2c`.

Validation adds real upstream calls. Compare their cost with the expensive work avoided;
a smaller count of business calls alone does not establish a net efficiency gain.

Recorded locally on 2026-10-15, Apple M2 Max, release builds, 100 trials and 20 ms owned
read delays. Baseline source is `d6c7e2c`; the candidate pins Wayframe `2e1ed6f`.
Timed trials begin with an external mutation and finish before TTL expiry.

| Mode | p50 / p95 / p99, ms | Stale results / 100 | Business / validation calls |
| --- | --- | ---: | ---: |
| Baseline TTL | 0.62 / 1.22 / 1.29 | 100 | 0 / 0 |
| Baseline uncached | 51.55 / 55.51 / 58.14 | 0 | 400 / 0 |
| Candidate TTL | 0.66 / 1.31 / 1.65 | 100 | 0 / 0 |
| Candidate validated | 3.71 / 30.04 / 33.44 | 0 | 40 / 600 |

Validated reuse avoided 90% of business calls while increasing total MCP calls from 400
to 640. The low TTL timings return stale data and are not equivalent optimizations.
Raw trials, CPU and sampled RSS are in `target/remote-state/benchmark.json`.

The same candidate passed all 4,000 state-workflow responses with 1,000 connected clients,
500 pending calls and a 64-call upstream limit. Warm runs issued zero business calls;
each single-branch change recomputed two nodes. Warm request p50/p95/p99 was
109.33/114.37/115.46 ms in the paced burst workload. Post-phase gateway RSS samples reached
139.92 MiB. Raw phases are in `target/remote-state/state-scale.json`.
The ordinary 1,000-client/500-concurrency qualification also passed with 10 seconds of
500-call/second offered load and 10% tracing; its sampled peak RSS was 327.61 MiB.
That short run does not replace the longer measurements above. Its report is
`target/remote-state/scale-qualification.json`.
