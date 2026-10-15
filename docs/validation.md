# Recorded qualification

Recorded 2026-10-14 and 2026-10-15 on macOS arm64, Apple M2 Max (12 CPUs, 32 GiB RAM), Rust 1.99.0, Python 3.14.7. Rust dependencies are locked in `Cargo.lock`; Wayframe is pinned to `ec4d27224ec213eabc2eae9b474b522d1cab8874`. These are local measurements. CI publishes separate Linux/macOS artifacts for each tested commit.

## Single-node service qualification (2026-10-15)

The 0.2.0-alpha.1 release binary passed the complete 1,000-client / 500-call profile in [the scale suite](scale.md), including 500 simultaneous workflows with 1,000 observed upstream branches. The profile launches burst callers in groups of 16 with a 2 ms pause and requires all target calls to overlap at the upstream barrier. It uses a 10 ms owned echo delay for the separately paced sustained workload.

| Tracing | Offered / achieved rate | Successful sustained calls | Latency p50 / p95 / p99 | Generator lag p99 | Sampled peak gateway RSS / FDs |
| --- | --- | ---: | --- | --- | --- |
| Off | 500 / 499.92 calls/s | 30,000 over 60 s | 10.86 / 16.94 / 71.56 ms | 35.80 ms | 284.55 MiB / 2,383 |

Every output matched its caller's nonce and owner. All 18 phases passed: admission/byte pressure, rate buckets, parallel DAGs and dependent effects, singleflight and cross-user isolation, uncertain effects without replay, 500 cancellations and deadlines, 3 × 1,000 session churn, queued revocation at a 16-call server quota, persistence across restart, active work surviving idle TTL, idle expiry, graceful shutdown with a nonreading client, and stdio input-error cleanup. Cleanup error/drop counts were zero at the required drains. The full profile and smoke test export raw results in CI; current sampled-tracing measurements are in the CI artifacts and pull-request validation evidence.

Before the idle-session fix, the same active-TTL regression failed on the preserved pre-fix binary with `active session rejected after idle TTL`. The current binary passes. Ten consecutive diagnostic runs of the capacity/failure phases also passed with the declared ramp. Unpaced connection bursts intermittently produced TCP resets before peer dispatch on this macOS host; that separate experiment remains available through `--ramp-ms 0` and is not claimed as a zero-error profile.

This is a workload-specific single-node qualification, not a production SLO. The load generator, owned upstream and collector share the host. Resource peaks are sampled, body accounting is not total RSS, and longer deployment-specific soaks remain necessary. The earlier forwarding and optimization measurements below are retained as dated fixture baselines.

Release binary SHA-256: `fe9660320408f04328f6fba0fdc4a7b3f0110c3c09aa66f1640ba9ab1f144b46`. Local raw report `target/scale-release.json`: `b317f6da7e6d22ac307a0e4674dac2870e65538208d3b4af8c272fd41aede568`. `target/scale-verification.log` records the current combined checks. CI artifacts contain independent Linux/macOS measurements for their own binaries and tested commit.

## Controls and interoperability

`bash scripts/verify.sh` passed:

- Formatting, 10 unit tests, 10 live fixture integration tests, Clippy with warnings denied and Python syntax checks.
- 44 live gateway checks covering DNS-independent fixture startup, real stdio branches and JSON/SSE upstreams, exact/partial reuse, learned preparation without invocation, grants, identity/session isolation, rate limits, singleflight, invalidation, malformed/oversized requests, cancellation, deadlines, admin CLI and revocation while an effect waits for a connection.
- The independent official Python SDK `mcp==2.0.1`, using its default negotiation against HTTP and stdio ingress, including a workflow and retained result reuse.
- Actual OTLP/HTTP JSON export to an ephemeral collector: 312 spans, including 101 graph nodes with dependency/scheduler/execution timing fields, gateway-to-upstream trace parentage, valid timestamps and executed/reused dispositions.

The owned counter advertises `readOnlyHint=true` but is not eligible for retention and still increments on each call. Tool execution errors are not retained even when the tool has an explicit reuse rule. A separate owned tool applies an effect and drops its HTTP response; the gateway reports failure, the effect count is exactly one, and a later distinct request can reconnect. This tests uncertain outcomes without claiming rollback or exactly-once remote execution.

The preparation-revocation regression was also run against previous commit `61b43c6`: it failed because one tool dispatched after revocation. The current implementation passes with zero calls by rechecking access after preparation. The older I/O test now asserts that dispatch count cannot grow after revocation instead of assuming both independently initialized processes reach their first call simultaneously.

## Actual MCP demo

The release demo starts owned HTTP and stdio MCP servers and sends real client requests through the gateway. Preparing two environments issued zero tool calls; workflow call counts were **4 → 0 → 2** for first execution, exact repeat and a source-only change. A mutable counter executed twice. This proves reusable per-tool branches and synchronous joining on the authored graph.

`python3 demos/mcp_demo.py target/debug/waypilot --remote` also completed an actual gateway call to `https://learn.microsoft.com/api/mcp`, invoking `learn__microsoft_docs_search`. The service returned one content item without a tool error, and the gateway reported `bypassed` for reuse. The query was fixed public documentation text. This is one live third-party integration, not broad server certification or a service availability promise.

## Forwarding overhead (earlier fixture snapshot)

Command: `python3 scripts/benchmark_gateway.py target/release/waypilot --samples 100`. Sessions were initialized, three warmup calls ran, measurement order alternated, and reuse was disabled. Each gateway mode made exactly 100 measured upstream invocations with outputs equivalent to direct calls. Trace export was off.

| Upstream response | Direct p50 / p95 | Gateway p50 / p95 | Difference of p50s |
| --- | ---: | ---: | ---: |
| JSON | 0.554 / 0.742 ms | 0.888 / 1.231 ms | +0.334 ms |
| SSE | 0.568 / 1.009 ms | 0.886 / 1.381 ms | +0.318 ms |

This includes caller HTTP/JSON overhead, the gateway's controls/graph and an additional HTTP exchange to the same owned loopback echo server. It is a sequential local workload, not a throughput benchmark, isolated scheduler cost, production tail or a remote-network estimate. The difference of medians is descriptive and is not a paired tail-overhead statistic.

## Retained fixture optimization comparison (2026-10-14)

Command: `target/release/waypilot demo --samples 20`. Model reasoning is a simulated 100 ms gap; fixture startup adds 60 ms and each tool adds 15 ms. Timings also include actual process startup, MCP transport and execution costs.

| Case | Local p50 | Local p95 | Calls/trial | Processes/trial |
| --- | ---: | ---: | ---: | ---: |
| Serial cold | 483.028 ms | 529.451 ms | 4 | 2 |
| Parallel cold | 322.327 ms | 351.988 ms | 4 | 2 |
| Parallel with learned prewarming | 204.135 ms | 233.098 ms | 4 | 2 |

All 60 trials produced equivalent outputs. Two prior training runs used eight calls and two process starts; their work is reported separately. Prewarming is included in each end-to-end interval and does not reduce tool-call count. The retained-state checks again issued four, zero and two calls. Those are behavioral cases, not cold-start latency distributions.

There is no model in these workloads. Automatic workflow mining, speculative tool execution, external mutable-resource invalidation, agent quality, distributed trace propagation and durable execution were not qualified.

## Raw evidence

Local raw files are ignored under `target/`. CI uploads its own `scale-smoke.json`, `benchmark.json`, `gateway-benchmark.json` and `mcp-demo.json` for both platforms, plus Linux `scale-qualification.json`; their measurements and hashes will differ.

| Local file | SHA-256 |
| --- | --- |
| `target/benchmark.json` | `39f268edca7e31b588da5e641797269bb9addf4f6f5a42d0d3b52f258969e81b` |
| `target/gateway-benchmark.json` | `73aae9930f3b79272a59605ac37e4f3e307523dde45607b064302b6feda01b9d` |
| `target/mcp-demo.json` | `e6966bdabfbc83f3b8bb82c890c18dd34ad0ac32bee440b4bd5a0e045c12c2ad` |
| `target/mcp-demo-remote.json` | `de0dabd5fed96223ff2280ecffa1c37be53b9b95e8357dab2377d95cc6b40f32` |

`target/prototype-verification.log` contains the earlier combined checks; `target/base-regression.log` records the failing previous-commit check. Raw output can be regenerated with the documented commands; only the optional third-party call depends on an external live service.
