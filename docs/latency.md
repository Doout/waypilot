# Latency experiments

The main cost in the owned workbench was repeated JSON schema-definition checking on
every RPC. Compiling schemas once removed that work; every call still validates its
arguments. Gateway changes alone produced smaller gains.

Measurements on 2026-10-17: macOS ARM64, Python 3.14.7, 128 policy agents at concurrency
32, 30 ms business tools and 1 ms validators. Each profile starts empty. Experiments
run in pairs of rounds with reversed profile order; ranges below describe those runs,
not confidence intervals.

| Guarded profile | Agent p95, ms | Agents/s | Result |
| --- | ---: | ---: | --- |
| Previous gateway, per-call schema checks | 1,204–1,309 | 27–28 | Baseline |
| Share immutable gateway catalogs | 1,162–1,180 | 29 | Small, variable gain |
| Compile peer schemas | 468–524 | 69–70 | Largest isolated gain; peer-side |
| Compile client and peer schemas; parallel validation | 418–426 | 82–83 | Faster request preparation and validation |
| Above, with owned MCP session reuse | 403–405 | About 117 | Fewer session handshakes |

Every guarded profile checked 3,072 observations through 2,048 validator RPCs, made
12 business calls and appended exactly 128 notes. Business observations and decisions
matched. Session reuse reduced session creation from 128 to 32; it did not omit tools,
validation or discovery. Completed-batch p95, including client admission wait and MCP
initialization, fell from about 4.5–4.6 seconds to 1.1 seconds. Wall time includes teardown.

With the same compiled schemas and session reuse, uncached execution achieved about
103 agents/s and p95 389–393 ms. Guarded reuse improved throughput but still had a slightly
higher p95. Uncached execution also lacks cross-call observation guards. These results
cover owned loopback services, not arbitrary MCP servers or live-model latency.

At 250 ms tool delay, pooled guarded reuse gave median 417–420 ms versus uncached
1,542–1,545 ms. The cold tail remained: p95 was 1,503–1,508 ms versus 1,672–1,678 ms.
Throughput rose from about 20 to 44–45 agents/s with identical business observations
and effects. Reuse helps warm branches without removing cold tool latency.

## What to enable

The owned workbench and runner compile schemas by default. A `--legacy-schemas` option
keeps the earlier baseline reproducible. Gateway catalogs share immutable definitions
without extending their lifetime or retaining authorization decisions.

Parallel validation is opt-in and bounded per request. It overlaps independent servers,
while calls within each server retain their existing order:

```sh
bash scripts/agent_demo.sh --batch-validation --guard-observations --validation-parallelism 4
```

MCP session reuse is an explicit benchmark workload for the owned, session-independent
tools. Each worker owns its client and rotates after 12 agents before receipt capacity
is exhausted. Do not transparently pool arbitrary servers: MCP session history may
change tool behavior. Different principals need separate clients.

## Tradeoffs and rejected methods

- Eight upstream slots increased guarded p95 to 1,435–1,556 ms. Tighter limits added
  queueing. Neither 16 nor 64 slots consistently beat the larger budget at concurrency 32.
- Moving shared admission after the session lock had inconsistent gains across three
  rounds and regressed the legacy-schema workload. That code was not retained.
- Client schema compilation alone reduced driver CPU but barely changed latency while
  the peer remained the bottleneck.
- Reducing agent concurrency improved active-agent latency but slowed batch completion.
  With compiled schemas and session reuse, 8 workers gave p95 about 252 ms at 82–83
  agents/s; 16 gave 314–316 ms at 106–109 agents/s; 32 gave 403–405 ms at about 117 agents/s.
- TCP_NODELAY was already enabled on both gateway ingress and its HTTP client.

No method caches current version checks, skips authorization, relaxes deadlines or
retries effects. Parallel-validation regressions cover malformed replies, rate limits,
revocation, cancellation, deadlines and post-effect conflicts. A rendezvous test requires
two independent validators to arrive before either can finish; serialized validation
fails that test before any note is written.

## Reproduce

Preserve a previous release binary, build the current one, then run:

```sh
cargo build --locked --release
target/sdk-venv/bin/python scripts/benchmark_latency.py target/release/waypilot \
  --baseline /path/to/previous/waypilot --runs 128 --concurrency 32 --rounds 2 \
  --output target/latency-study.json
```

`--profile NAME` narrows the experiment. Use `--concurrency 8` or `16` to compare admission
wait and batch completion, and `--delay-ms 250` to test slower tools. Reports retain
p50/p95/p99, CPU by process, sampled gateway RSS/descriptors, RPCs, session creation,
startup time and drained resources. Timings are descriptive; output, work and control
invariants fail the run if they differ.

OTLP `mcp.invoke` spans now separate session-lock `queue_us` from `exchange_us`.
`mcp.state.validate_set` times independent validation groups. Arguments, results, receipts
and opaque resource versions remain excluded from trace fields.
