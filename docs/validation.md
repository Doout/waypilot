# Prototype qualification

Recorded 2026-10-14 on macOS arm64, Apple M2 Max, Rust 1.99.0, Python 3.14.7. Rust dependencies are locked in `Cargo.lock`; Wayframe is pinned to `ec4d27224ec213eabc2eae9b474b522d1cab8874`. These are local measurements. CI publishes separate Linux/macOS artifacts for each tested commit.

## Controls and interoperability

`bash scripts/verify.sh` passed:

- Formatting, 7 unit tests, 10 live fixture integration tests, Clippy with warnings denied and Python syntax checks.
- 43 live gateway checks covering real stdio branches and JSON/SSE upstreams, exact/partial reuse, learned preparation without invocation, grants, identity/session isolation, rate limits, singleflight, invalidation, malformed/oversized requests, cancellation, deadlines, admin CLI and revocation while an effect waits for a connection.
- The independent official Python SDK `mcp==2.0.1`, using its default negotiation against HTTP and stdio ingress, including a workflow and retained result reuse.
- Actual OTLP/HTTP JSON export to an ephemeral collector: 312 spans, including 101 graph nodes with dependency/scheduler/execution timing fields, gateway-to-upstream trace parentage, valid timestamps and executed/reused dispositions.

The owned counter advertises `readOnlyHint=true` but is not eligible for retention and still increments on each call. Tool execution errors are not retained even when the tool has an explicit reuse rule. A separate owned tool applies an effect and drops its HTTP response; the gateway reports failure, the effect count is exactly one, and a later distinct request can reconnect. This tests uncertain outcomes without claiming rollback or exactly-once remote execution.

The preparation-revocation regression was also run against previous commit `61b43c6`: it failed because one tool dispatched after revocation. The current implementation passes with zero calls by rechecking access after preparation. The older I/O test now asserts that dispatch count cannot grow after revocation instead of assuming both independently initialized processes reach their first call simultaneously.

## Actual MCP demo

The release demo starts owned HTTP and stdio MCP servers and sends real client requests through the gateway. Preparing two environments issued zero tool calls; workflow call counts were **4 → 0 → 2** for first execution, exact repeat and a source-only change. A mutable counter executed twice. This proves reusable per-tool branches and synchronous joining on the authored graph.

`python3 demos/mcp_demo.py target/debug/waypilot --remote` also completed an actual gateway call to `https://learn.microsoft.com/api/mcp`, invoking `learn__microsoft_docs_search`. The service returned one content item without a tool error, and the gateway reported `bypassed` for reuse. The query was fixed public documentation text. This is one live third-party integration, not broad server certification or a service availability promise.

## Forwarding overhead

Command: `python3 scripts/benchmark_gateway.py target/release/waypilot --samples 100`. Sessions were initialized, three warmup calls ran, measurement order alternated, and reuse was disabled. Each gateway mode made exactly 100 measured upstream invocations with outputs equivalent to direct calls. Trace export was off.

| Upstream response | Direct p50 / p95 | Gateway p50 / p95 | Difference of p50s |
| --- | ---: | ---: | ---: |
| JSON | 0.554 / 0.742 ms | 0.888 / 1.231 ms | +0.334 ms |
| SSE | 0.568 / 1.009 ms | 0.886 / 1.381 ms | +0.318 ms |

This includes caller HTTP/JSON overhead, the gateway's controls/graph and an additional HTTP exchange to the same owned loopback echo server. It is a sequential local workload, not a throughput benchmark, isolated scheduler cost, production tail or a remote-network estimate. The difference of medians is descriptive and is not a paired tail-overhead statistic.

## Fixture optimization comparison

Command: `target/release/waypilot demo --samples 20`. Model reasoning is a simulated 100 ms gap; fixture startup adds 60 ms and each tool adds 15 ms. Timings also include actual process startup, MCP transport and execution costs.

| Case | Local p50 | Local p95 | Calls/trial | Processes/trial |
| --- | ---: | ---: | ---: | ---: |
| Serial cold | 483.028 ms | 529.451 ms | 4 | 2 |
| Parallel cold | 322.327 ms | 351.988 ms | 4 | 2 |
| Parallel with learned prewarming | 204.135 ms | 233.098 ms | 4 | 2 |

All 60 trials produced equivalent outputs. Two prior training runs used eight calls and two process starts; their work is reported separately. Prewarming is included in each end-to-end interval and does not reduce tool-call count. The retained-state checks again issued four, zero and two calls. Those are behavioral cases, not cold-start latency distributions.

There is no model in these workloads. Automatic workflow mining, speculative tool execution, external mutable-resource invalidation, agent quality, distributed trace propagation and durable execution were not qualified.

## Raw evidence

Local raw files are ignored under `target/`. CI uploads its own `benchmark.json`, `gateway-benchmark.json` and `mcp-demo.json` for both platforms; their measurements and hashes will differ.

| Local file | SHA-256 |
| --- | --- |
| `target/benchmark.json` | `39f268edca7e31b588da5e641797269bb9addf4f6f5a42d0d3b52f258969e81b` |
| `target/gateway-benchmark.json` | `73aae9930f3b79272a59605ac37e4f3e307523dde45607b064302b6feda01b9d` |
| `target/mcp-demo.json` | `e6966bdabfbc83f3b8bb82c890c18dd34ad0ac32bee440b4bd5a0e045c12c2ad` |
| `target/mcp-demo-remote.json` | `de0dabd5fed96223ff2280ecffa1c37be53b9b95e8357dab2377d95cc6b40f32` |

`target/prototype-verification.log` contains the combined checks; `target/base-regression.log` records the failing previous-commit check. Raw output can be regenerated with the documented commands; only the optional third-party call depends on an external live service.
