# Benchmark and qualification strategy

## M0 workload

Two MCP servers run local deterministic tools over immutable content snapshots. Each server has two dependent calls; the two server branches are independent. A server starts with a 60 ms fixture delay, each tool has a 15 ms fixture delay, and a 100 ms simulated reasoning gap precedes authoritative work. Real subprocess startup, MCP handshake, serialization, scheduling, tracing and cache operations remain measured.

Compare:

1. Serial cold branches.
2. Ordinary parallel cold branches with the same tool calls and outputs.
3. The same parallel graph with environment prewarming learned from two successful training runs, overlapping the simulated gap.
4. An identical request with exact branch result reuse.
5. A source-only change with one retained branch and one recomputed branch.

Cases 1–3 use fresh engines per trial and disable result reuse. Prewarming is charged to end-to-end elapsed time; process starts and tool counts are reported. Training has separate reported work. Cases 4–5 use the same engine intentionally, to test retained state. Do not present their latency as a cold-start comparison.

The demo verifies equivalent baseline outputs and asserts zero calls on a full reuse hit and exactly two calls after a source-only change. It emits raw trials, nearest-rank p50/p95 and graph node timings. Low sample counts describe a local fixture, not production tails. Cache-hit measurements do not include a model call because this workload has no model.

## Subsequent comparison matrix

Keep the model, tool snapshots, prompts, output order, transport, concurrency limits and hardware fixed while comparing ordinary parallel execution, exact result reuse alone, static prewarming/prefetch, learned bounded preparation/speculation and qualified compiled fragments. Include no-benefit workloads and measure prediction/compilation/training overhead rather than just successful warm hits.

Report tool stall time, end-to-end p50/p95/p99, task success, model calls/tokens, process starts, memory, upstream concurrency, extra calls, speculative hit/waste, invalidation/fallback reasons and break-even request count. Benchmark tracing off/on separately. Do not sum overlapping span durations to claim elapsed-time savings.

Hold out tasks and branches before training. Add misses, slow siblings, queue saturation, revoked grants, changed tool contracts, external revisions, stale values, canceled consumers, crashed workers and unknown effects. Reuse is acceptable only within the declared freshness contract; permission or freshness violations are failures even when latency improves.

A replay with simulated model timing establishes scheduling behavior. A later controlled run with a real model and outcome verifier is required before claiming an improvement to agent performance or quality. Synthetic tool mocks are labeled by the semantics they cover.
