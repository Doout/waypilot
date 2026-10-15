# Wayframe evolution required by Waypilot

Reviewed against Wayframe commit `ec4d27224ec213eabc2eae9b474b522d1cab8874`.

## Existing foundations

`wayframe-core` supplies application-owned contexts, mandatory decisions and deadline contracts. `wayframe-flow` supplies typed immutable graph topology, explicit branch selectors, choices, joins, gates, node budgets, prepared output plans and ready-node scheduling. Prepared plans retain structure only. Current invocation facts and authorization are evaluated afresh.

`wayframe-runtime` supplies embedded pipeline/adaptive execution. `wayframe-transport` supplies supervised stdio processes and explicit policy transport bridges. Its policy envelopes are not a generic distributed task protocol. `wayframe-observability` supplies optional OTLP export.

`wayframe-cache` already supplies a typed bounded result store, canonical scoped keys, generation invalidation and singleflight guards. The live MCP gateway uses it directly.

Waypilot uses these public crates without changing Wayframe. It is an external consumer with a pinned Git dependency, not a copied scheduler or a second framework implementation.

## Proposed additions

| Priority | Primitive and likely home | Required semantics and validation |
| --- | --- | --- |
| 1 | Dependency witnesses and selective invalidation; extend `wayframe-cache` | Reuse its existing typed store, canonical scope/generation keys, entry/byte/TTL bounds and singleflight. Add explicit dependency revision vectors and validation reasons only when a working scenario requires them. No stored authorization results or inferred eligibility. Test nested dependencies, revision changes, eviction and concurrency. |
| 2 | Work leases and admission; `wayframe-runtime` | Separate authoritative/speculative budgets, bounded pending work, reference-counted consumers, individual deadlines, cancellation ownership and explicit promotion. Test one consumer leaving, all consumers leaving, fairness and capacity saturation. |
| 2 | Session lifecycle; `wayframe-transport` | Model process/session readiness, configuration/credential compatibility, drain, shutdown and replacement. Current `stdio::Pending::drop` cancels the whole process when a response is still pending: do not share such a speculative exchange with committed work. Preserve fail-closed behavior until per-call cancellation is qualified. |
| 3 | Reusable fragment boundary; `wayframe-flow` | Keep entry and release gates outside retained data results. Preserve current selector and deadline checks. Expose executed/reused/joined/invalidated dispositions without treating an omitted control as a cache hit. Test nested graph controls and synchronous work after joins. |
| 3 | Continuation regions; `wayframe-flow` + `wayframe-runtime` | Submit a new validated immutable region after an existing region produces facts. Preserve original deadline, accumulated budgets, control ancestry and generation ownership. Avoid mutation of a live topology. Validate missing/foreign dependencies, loops and denied parent regions. |
| 4 | Execution record interface; `wayframe-runtime` | Stable invocation/attempt IDs, completed results, canceled and uncertain-effect outcomes; bounded retention and pluggable persistence. Do not claim exactly-once external effects. Test restart at every commit boundary and application reconciliation. |
| Throughout | Generic event fields; flow/runtime/observability | Region/fragment version, decision reason, dependency/scheduler/execution time, reuse validation, lease ownership and speculative waste. Keep payloads/credentials out of default traces; distinguish elapsed critical-path time from sums of overlapping spans. |

These names are design proposals, not committed public APIs. Build the smallest standalone primitive first and extract only behavior validated in both Waypilot and a non-agent fixture.

## Boundaries to preserve

- Native blocks remain trusted cooperative code; this project does not turn them into a sandbox.
- Measured priorities reorder ready independent nodes and never cross dependencies, gates or deadlines.
- Prepared plan reuse and retained result reuse have separate lifecycles. A valid structural plan says nothing about the validity of an old result.
- Unknown dependencies remain explicit ordering constraints until contracts resolve them. A missing trace edge is not proof of independence.
- Shared computation does not imply shared credentials or permission to disclose data to another consumer.
- Network MCP, local stdio, embedded tasks and external policy bridges have distinct lifecycle semantics. A transport adapter must declare and test its capabilities.
- Durable distributed scheduling is a separate extension; do not make a database or daemon mandatory for an embedded library.

## Cross-repository change process

1. Add a failing Waypilot scenario against the pinned framework revision.
2. Reproduce the missing generic behavior in a small Wayframe test and a non-agent use case.
3. Implement a narrow optional primitive while preserving current default behavior.
4. Run Wayframe's canonical `bash scripts/verify.sh` and relevant release benchmarks for framework code changes.
5. Pin the committed candidate revision in Waypilot and run integration, trace and baseline checks.
6. Land the framework change, update the dependency pin and record qualification evidence in both repositories.

No framework code change is required for the current MCP gateway. Existing cache, graph, transport and telemetry APIs cover this prototype. Dependency witnesses and work leases are the next candidate extensions; do not introduce a duplicate reuse crate.
